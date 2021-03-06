import csv
import datetime
import glob
import logging
import netCDF4
import numpy
import os
import pdb
import sys
import time
import xlrd

# Import custom modules
sys.path.append('/mnt/PyFluxPro_V0.2.0/scripts')
import constants as c
import grunt_email
import meteorologicalfunctions as mf
import pfp_io
import pfp_ts
import pfp_utils

mail_recipients = ['ian.mchugh@monash.edu']

###############################################################################
# Functions                                                                   #
###############################################################################

#------------------------------------------------------------------------------

def downsample_aws(in_path, master_file_pathname):
    
    wb = xlrd.open_workbook(master_file_pathname)
    sheet = wb.sheet_by_name('Active')
    site_list = sheet.col_values(0, 10)
    time_step_list = sheet.col_values(8, 10)
    sites_list = [''.join(site_list[i].split(' ')) for i, time_step 
                  in enumerate(time_step_list) if time_step == 60]

    for site in sites_list:

        logging.info('Running timestep conversion for {} site:'.format(site))
        path = os.path.join(in_path, site, "Data/AWS")
        current_f = '{}_AWS.nc'.format(site)
        aws_name = os.path.join(path, current_f)
        ncobj = netCDF4.Dataset(aws_name)
        attr_dict = ncobj.__dict__
        if int(attr_dict['time_step']) == 60:
            logging.info('Conversion already complete!')
            continue
        logging.info('Conversion required! Starting now...')
        ds_aws_30minute = pfp_io.nc_read_series(aws_name)
        has_gaps = pfp_utils.CheckTimeStep(ds_aws_30minute)
        if has_gaps:
            print "Problems found with time step"
            pfp_utils.FixTimeStep(ds_aws_30minute)
            pfp_utils.get_ymdhmsfromdatetime(ds_aws_30minute)
        dt_aws_30minute = ds_aws_30minute.series["DateTime"]["Data"]
        ddt=[dt_aws_30minute[i+1]-dt_aws_30minute[i] for i in range(0,len(dt_aws_30minute)-1)]
        print "Minimum time step is",min(ddt)," Maximum time step is",max(ddt)
        
        dt_aws_30minute = ds_aws_30minute.series["DateTime"]["Data"]
        start_date = dt_aws_30minute[0]
        end_date = dt_aws_30minute[-1]
        si_wholehour = pfp_utils.GetDateIndex(dt_aws_30minute,str(start_date),ts=30,match="startnexthour")
        ei_wholehour = pfp_utils.GetDateIndex(dt_aws_30minute,str(end_date),ts=30,match="endprevioushour")
        start_date = dt_aws_30minute[si_wholehour]
        end_date = dt_aws_30minute[ei_wholehour]
        dt_aws_30minute_array = numpy.array(dt_aws_30minute[si_wholehour:ei_wholehour+1])
        nRecs_30minute = len(dt_aws_30minute_array)
        dt_aws_2d = numpy.reshape(dt_aws_30minute_array,(nRecs_30minute/2,2))
        dt_aws_60minute = list(dt_aws_2d[:,1])
        nRecs_60minute = len(dt_aws_60minute)   
        series_list = ds_aws_30minute.series.keys()
        
        for item in ["DateTime","Ddd","Day","Minute","xlDateTime","Hour","time","Month","Second","Year"]:
            if item in series_list: series_list.remove(item)
            
        # get the 60 minute data structure
        ds_aws_60minute = pfp_io.DataStructure()
        # get the global attributes
        for item in ds_aws_30minute.globalattributes.keys():
            ds_aws_60minute.globalattributes[item] = ds_aws_30minute.globalattributes[item]
        # overwrite with 60 minute values as appropriate
        ds_aws_60minute.globalattributes["nc_nrecs"] = str(nRecs_60minute)
        ds_aws_60minute.globalattributes["time_step"] = str(60)
        # put the Python datetime into the data structure
        ds_aws_60minute.series["DateTime"] = {}
        ds_aws_60minute.series["DateTime"]["Data"] = dt_aws_60minute
        ds_aws_60minute.series["DateTime"]["Flag"] = numpy.zeros(nRecs_60minute,dtype=numpy.int32)
        ds_aws_60minute.series["DateTime"]["Attr"] = pfp_utils.MakeAttributeDictionary(long_name="DateTime in local time zone",units="None")
        # add the Excel datetime, year, month etc
        pfp_utils.get_xldatefromdatetime(ds_aws_60minute)
        pfp_utils.get_ymdhmsfromdatetime(ds_aws_60minute)
        # loop over the series and take the average (every thing but Precip) or sum (Precip)
        for item in series_list:
            if "Precip" in item:
                data_30minute,flag_30minute,attr = pfp_utils.GetSeriesasMA(ds_aws_30minute,item,si=si_wholehour,ei=ei_wholehour)
                data_2d = numpy.reshape(data_30minute,(nRecs_30minute/2,2))
                flag_2d = numpy.reshape(flag_30minute,(nRecs_30minute/2,2))
                data_60minute = numpy.ma.sum(data_2d,axis=1)
                flag_60minute = numpy.ma.max(flag_2d,axis=1)
                pfp_utils.CreateSeries(ds_aws_60minute,item,data_60minute,Flag=flag_60minute,Attr=attr)
            elif "Wd" in item:
                ws_name = item.replace('d', 's')
                if ws_name in series_list:
                    Wd_30minute = pfp_utils.GetVariable(ds_aws_30minute, item, start_date, end_date)
                    Ws_30minute = pfp_utils.GetVariable(ds_aws_30minute, ws_name, start_date, end_date)
                    U_30minute,V_30minute = pfp_utils.convert_WSWDtoUV(Ws_30minute,Wd_30minute)
                    U_2d = numpy.reshape(U_30minute['Data'],(nRecs_30minute/2,2))
                    V_2d = numpy.reshape(V_30minute['Data'],(nRecs_30minute/2,2))
                    flag_2d = numpy.reshape(Wd_30minute['Flag'] + Ws_30minute['Flag'],(nRecs_30minute/2,2))
                    U_60minute = numpy.ma.sum(U_2d,axis=1)
                    V_60minute = numpy.ma.sum(V_2d,axis=1)
                    flag_60minute = numpy.ma.max(flag_2d,axis=1)
                    Ws_60minute,Wd_60minute = pfp_utils.convert_UVtoWSWD({'Data': U_60minute} ,{'Data': V_60minute})
                    pfp_utils.CreateSeries(ds_aws_60minute,item,Wd_60minute['Data'],Flag=flag_60minute,Attr=Wd_30minute['Attr'])
                    pfp_utils.CreateSeries(ds_aws_60minute,ws_name,Ws_60minute['Data'],Flag=flag_60minute,Attr=Ws_30minute['Attr'])
            elif "Ws" in item:
                continue
            else:
                data_30minute,flag_30minute,attr = pfp_utils.GetSeriesasMA(ds_aws_30minute,item,si=si_wholehour,ei=ei_wholehour)
                data_2d = numpy.reshape(data_30minute,(nRecs_30minute/2,2))
                flag_2d = numpy.reshape(flag_30minute,(nRecs_30minute/2,2))
                data_60minute = numpy.ma.average(data_2d,axis=1)
                flag_60minute = numpy.ma.max(flag_2d,axis=1)
                pfp_utils.CreateSeries(ds_aws_60minute,item,data_60minute,Flag=flag_60minute,Attr=attr)  

        # write out the 60 minute data
        aws_30min_name = aws_name.replace('.nc','_30minute.nc')
        if os.path.isfile(aws_30min_name):
            os.remove(aws_30min_name)
        os.rename(aws_name, aws_30min_name) 
        ncfile = pfp_io.nc_open_write(aws_name)
        pfp_io.nc_write_series(ncfile, ds_aws_60minute, ndims=1)
        print 'Conversion complete!'
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------

def aws_to_nc(in_path, out_path, master_file_pathname):
       
    # dummy control file for FixTimeSteps
#    cf = {"Options":{"FixTimeStepMethod":"round"}}
    
    # get the site information and the AWS stations to use
    wb = xlrd.open_workbook(master_file_pathname)
    sheet = wb.sheet_by_name("Active")
    header_row = 9
    start_row = 10
    header_list = sheet.row_values(header_row)
    alias_dict = {'Name': 'site_name',
                  'Altitude': 'elevation'}
    bom_sites_info = {}
    for row_num in range(start_row, sheet.nrows):
        this_row = sheet.row_values(row_num)
        ozflux_site_name = this_row[header_list.index('Site')]
        bom_sites_info[ozflux_site_name] = {}
        for var in ['Latitude', 'Longitude', 'Altitude']:
            var_col_idx = header_list.index(var)
            try:
                var = alias_dict[var]
            except:
                pass
            bom_sites_info[ozflux_site_name][var.lower()] = this_row[var_col_idx]
        for index in range(1, 5):
            stn_col_idx = header_list.index('BoM_ID_{}'.format(str(index)))
            try:
                bom_id = str(int(this_row[stn_col_idx]))
                bom_sites_info[ozflux_site_name][bom_id] = {}
                for var in ['Latitude', 'Longitude', 'Altitude', 
                            'Distance', 'Name']:
                    col_idx = header_list.index('{}_{}'.format(var, str(index)))
                    try:
                        var = alias_dict[var]
                    except KeyError:
                        pass
                    bom_sites_info[ozflux_site_name][bom_id][var.lower()] = this_row[col_idx]
            except:
                continue

    in_filename = os.path.join(in_path, 'HM01X_Data*.txt')
    file_list = sorted(glob.glob(in_filename))
    site_list = bom_sites_info.keys()
    for site_name in sorted(site_list):
        logging.info("Starting site: "+site_name)
        sname = site_name.replace(" ","")
        site_out_path = os.path.join(out_path, sname, "Data/AWS")
        ncname = os.path.join(site_out_path, "{}_AWS.nc".format(sname))
        site_latitude = bom_sites_info[site_name]["latitude"]
        site_longitude = bom_sites_info[site_name]["longitude"]
        site_elevation = bom_sites_info[site_name]["elevation"]
        site_number_list = bom_sites_info[site_name].keys()
        for item in ["latitude","longitude","elevation"]:
            if item in site_number_list: site_number_list.remove(item)
        # read the CSV files and put the contents into data_dict
        data_dict = {}
        for idx,sn in enumerate(site_number_list):
            # get a list of file names that contain the relevent station numbers
            csvname = [fn for fn in file_list if str(sn) in fn]
            # continue to next station if this station not in file_list
            if len(csvname)==0: continue
            logging.info("Reading CSV file: "+str(csvname[0]))
            # columns are:
            # file data content
            #  1    0    station number
            #  7    1    year, local standard time
            #  8    2    month, local standard time
            #  9    3    day, local standard time
            #  10   4    hour, local standard time
            #  11   5    minute, local standard time
            #  12   6    precip since 0900
            #  14   7    air temperature, C
            #  16   8    dew point temperature, C
            #  18   9    relative humidity, %
            #  20   10   wind speed, m/s
            #  22   11   wind direction, degT
            #  24   12   gust in last 10 minutes, m/s
            #  26   13   station pressure, hPa
            data=numpy.genfromtxt(csvname[0],skip_header=1,delimiter=",",usecols=(1,7,8,9,10,11,12,14,16,18,20,22,24,26),
                                  missing_values=-9999,filling_values=-9999)
            data = numpy.ma.masked_equal(data,float(-9999),copy=True)
            data_dict[sn] = data

        # now pull the data out and put it in separate data structures, one per station, all
        # of which are held in a data structure dictionary
        ds_dict = {}
        for bom_id in data_dict.keys():
            logging.info("Processing BoM station: "+str(bom_id))
            # create a data structure
            ds=pfp_io.DataStructure()
            # put the year, month, day, hour and minute into the data structure
            nRecs = data_dict[bom_id].shape[0]
            ds.globalattributes["nc_nrecs"] = nRecs
            ds.globalattributes["time_step"] = 30
            ds.globalattributes["latitude"] = bom_sites_info[site_name][str(bom_id)]["latitude"]
            ds.globalattributes["longitude"] = bom_sites_info[site_name][str(bom_id)]["longitude"]
            flag = numpy.zeros(nRecs,dtype=numpy.int32)
            Seconds = numpy.zeros(nRecs,dtype=numpy.float64)
            pfp_utils.CreateSeries(ds,'Year',data_dict[bom_id][:,1],Flag=flag,Attr=pfp_utils.MakeAttributeDictionary(long_name='Year',units='none'))
            pfp_utils.CreateSeries(ds,'Month',data_dict[bom_id][:,2],Flag=flag,Attr=pfp_utils.MakeAttributeDictionary(long_name='Month',units='none'))
            pfp_utils.CreateSeries(ds,'Day',data_dict[bom_id][:,3],Flag=flag,Attr=pfp_utils.MakeAttributeDictionary(long_name='Day',units='none'))
            pfp_utils.CreateSeries(ds,'Hour',data_dict[bom_id][:,4],Flag=flag,Attr=pfp_utils.MakeAttributeDictionary(long_name='Hour',units='none'))
            pfp_utils.CreateSeries(ds,'Minute',data_dict[bom_id][:,5],Flag=flag,Attr=pfp_utils.MakeAttributeDictionary(long_name='Minute',units='none'))
            pfp_utils.CreateSeries(ds,'Second',Seconds,Flag=flag,Attr=pfp_utils.MakeAttributeDictionary(long_name='Second',units='none'))
            # now get the Python datetime 
            pfp_utils.get_datetimefromymdhms(ds)
            # now put the data into the data structure
            attr=pfp_utils.MakeAttributeDictionary(long_name='Precipitation since 0900',units='mm',
                                                 bom_id=str(bom_id),bom_name=bom_sites_info[site_name][str(bom_id)]["site_name"],
                                                 bom_dist=bom_sites_info[site_name][str(bom_id)]["distance"])
            pfp_utils.CreateSeries(ds,'Precip',data_dict[bom_id][:,6],Flag=flag,Attr=attr)
            attr=pfp_utils.MakeAttributeDictionary(long_name='Air temperature',units='C',
                                                 bom_id=str(bom_id),bom_name=bom_sites_info[site_name][str(bom_id)]["site_name"],
                                                 bom_dist=bom_sites_info[site_name][str(bom_id)]["distance"])
            pfp_utils.CreateSeries(ds,'Ta',data_dict[bom_id][:,7],Flag=flag,Attr=attr)
            attr=pfp_utils.MakeAttributeDictionary(long_name='Dew point temperature',units='C',
                                                 bom_id=str(bom_id),bom_name=bom_sites_info[site_name][str(bom_id)]["site_name"],
                                                 bom_dist=bom_sites_info[site_name][str(bom_id)]["distance"])
            pfp_utils.CreateSeries(ds,'Td',data_dict[bom_id][:,8],Flag=flag,Attr=attr)
            attr=pfp_utils.MakeAttributeDictionary(long_name='Relative humidity',units='%',
                                                 bom_id=str(bom_id),bom_name=bom_sites_info[site_name][str(bom_id)]["site_name"],
                                                 bom_dist=bom_sites_info[site_name][str(bom_id)]["distance"])
            pfp_utils.CreateSeries(ds,'RH',data_dict[bom_id][:,9],Flag=flag,Attr=attr)
            attr=pfp_utils.MakeAttributeDictionary(long_name='Wind speed',units='m/s',
                                                 bom_id=str(bom_id),bom_name=bom_sites_info[site_name][str(bom_id)]["site_name"],
                                                 bom_dist=bom_sites_info[site_name][str(bom_id)]["distance"])
            pfp_utils.CreateSeries(ds,'Ws',data_dict[bom_id][:,10],Flag=flag,Attr=attr)
            attr=pfp_utils.MakeAttributeDictionary(long_name='Wind direction',units='degT',
                                                 bom_id=str(bom_id),bom_name=bom_sites_info[site_name][str(bom_id)]["site_name"],
                                                 bom_dist=bom_sites_info[site_name][str(bom_id)]["distance"])
            pfp_utils.CreateSeries(ds,'Wd',data_dict[bom_id][:,11],Flag=flag,Attr=attr)
            attr=pfp_utils.MakeAttributeDictionary(long_name='Wind gust',units='m/s',
                                                 bom_id=str(bom_id),bom_name=bom_sites_info[site_name][str(bom_id)]["site_name"],
                                                 bom_dist=bom_sites_info[site_name][str(bom_id)]["distance"])
            pfp_utils.CreateSeries(ds,'Wg',data_dict[bom_id][:,12],Flag=flag,Attr=attr)
            data_dict[bom_id][:,13] = data_dict[bom_id][:,13]/float(10)
            attr=pfp_utils.MakeAttributeDictionary(long_name='Air Pressure',units='kPa',
                                                 bom_id=str(bom_id),bom_name=bom_sites_info[site_name][str(bom_id)]["site_name"],
                                                 bom_dist=bom_sites_info[site_name][str(bom_id)]["distance"])
            pfp_utils.CreateSeries(ds,'ps',data_dict[bom_id][:,13],Flag=flag,Attr=attr)
            # fix any time stamp issues
            if pfp_utils.CheckTimeStep(ds):
                pfp_utils.FixTimeStep(ds)
                # update the Year, Month, Day etc from the Python datetime
                pfp_utils.get_ymdhmsfromdatetime(ds)
            # now interpolate
            for label in ["Precip","Ta","Td","RH","Ws","Wd","Wg","ps"]:
                pfp_ts.InterpolateOverMissing(ds,series=label,maxlen=2)
            # put this stations data into the data structure dictionary
            ds_dict[bom_id] = ds
        # get the earliest start datetime and the latest end datetime
        logging.info("Finding the start and end dates")
        bom_id_list = ds_dict.keys()
        ds0 = ds_dict[bom_id_list[0]]
        ldt = ds0.series["DateTime"]["Data"]
        #print bom_id_list[0],":",ldt[0],ldt[-1]
        start_date = ldt[0]
        end_date = ldt[-1]
        bom_id_list.remove(bom_id_list[0])
        for bom_id in bom_id_list:
            dsn = ds_dict[bom_id]
            ldtn = dsn.series["DateTime"]["Data"]
            #print bom_id,":",ldtn[0],ldtn[-1]
            start_date = min([start_date,ldtn[0]])
            end_date = max([end_date,ldtn[-1]])
        #print start_date,end_date

        # merge the individual data structures into a single one
        logging.info("Merging file contents")
        ds_all = pfp_io.DataStructure()
        ds_all.globalattributes["time_step"] = 30
        ds_all.globalattributes["xl_datemode"] = 0
        ds_all.globalattributes["site_name"] = site_name
        ds_all.globalattributes["latitude"] = site_latitude
        ds_all.globalattributes["longitude"] = site_longitude
        ds_all.globalattributes["elevation"] = site_elevation
        ts = int(ds_all.globalattributes["time_step"])
        ldt_all = numpy.array([result for result in pfp_utils.perdelta(start_date,end_date,datetime.timedelta(minutes=ts))])
        nRecs = len(ldt_all)
        ds_all.globalattributes["nc_nrecs"] = nRecs
        ds_all.series["DateTime"] = {}
        ds_all.series["DateTime"]["Data"] = ldt_all
        flag = numpy.zeros(nRecs,dtype=numpy.int32)
        ds_all.series["DateTime"]["Flag"] = flag
        ds_all.series["DateTime"]["Attr"] = {}
        ds_all.series['DateTime']["Attr"]["long_name"] = "Date-time object"
        ds_all.series['DateTime']["Attr"]["units"] = "None"
        # get the year, month, day, hour, minute and seconds from the Python datetime
        pfp_utils.get_ymdhmsfromdatetime(ds_all)
        # get the xlDateTime from the 
        xlDateTime = pfp_utils.get_xldatefromdatetime(ds_all)
        attr = pfp_utils.MakeAttributeDictionary(long_name="Date/time in Excel format",units="days since 1899-12-31 00:00:00")
        pfp_utils.CreateSeries(ds_all,"xlDateTime",xlDateTime,Flag=flag,Attr=attr)
        # loop over the stations
        for idx,bom_id in enumerate(ds_dict.keys()):
            logging.info("Merging BoM site: "+str(bom_id))
            ds = ds_dict[bom_id]
            ldt = ds.series["DateTime"]["Data"]
            index = pfp_utils.FindMatchingIndices(ldt,ldt_all)[1]
            # loop over the variables
            for label in ["Precip","Ta","Td","RH","Ws","Wd","Wg","ps"]:
                data_all = numpy.ma.ones(nRecs,dtype=numpy.float64)*float(c.missing_value)
                flag_all = numpy.zeros(nRecs,dtype=numpy.int32)
                data,flag,attr = pfp_utils.GetSeriesasMA(ds,label)
                data_all[index] = data
                flag_all[index] = flag
                output_label = label+"_"+str(idx)
                attr["bom_id"] = str(bom_id)
                pfp_utils.CreateSeries(ds_all,output_label,data_all,Flag=flag_all,Attr=attr)
        # get precipitation per time step
        # now get precipitation per time step from the interpolated precipitation accumulated over the day
        precip_list = [x for x in ds_all.series.keys() if ("Precip" in x) and ("_QCFlag" not in x)]
        #print precip_list
        logging.info("Converting 24 hour accumulated precipitation")
        for output_label in precip_list:
            # getthe accumlated precipitation
            accum_data,accum_flag,accum_attr = pfp_utils.GetSeriesasMA(ds_all,output_label)
            # make the flag a masked array
            accum_flag = numpy.ma.array(accum_flag)
            # round small precipitations to 0
            index = numpy.ma.where(accum_data<0.01)[0]
            accum_data[index] = float(0)
            # get the precipitation per time step
            precip = numpy.ma.ediff1d(accum_data,to_begin=0)
            # trap the times when the accumlated precipitation is reset
            # This should be at a standard time every day for all BoM sites but this is not the case
            # For eaxample, at 72161, there is a period in 09/2010 when the reset seems to occur at
            # 1000 instead of the expected 0900 (possible daylight saving error?)
            # To get around this, we check the differentiated precipitation:
            # - if the precipitation per time step is positive, do nothing
            # - if the precipitation per time step is negative;
            #   - if the QC flag is 50 (linear interpolation) then set the precipitation to 0
            #   - if the QC flag is 0 then set the precipitation per time step to the accumlated
            #     precipitation
            # find times when the precipitation per time step is negative and the QC flag is not 0 ...
            index = numpy.ma.where((precip<0)&(accum_flag!=0))[0]
            # ... and set the precipitation per time step for these times to 0
            precip[index] = float(0)
            # find any remaining times when the precipitation per time step is negative ...
            index = numpy.ma.where(precip<0)[0]
            # ... and set them to the accumulated precipitation
            precip[index] = accum_data[index]
            #index = [x for x in range(len(ldt_all)) if (ldt_all[x].hour==8) and (ldt_all[x].minute==30)]
            #precip[index] = float(0)
            #index = [x for x in range(len(ldt_all)) if (ldt_all[x].hour==9) and (ldt_all[x].minute==0)]
            #precip[index] = accum_24hr[index]
            # set attributes as appropriate
            accum_attr["long_name"] = "Precipitation total over time step"
            accum_attr["units"] = "mm/30 minutes"
            # put the precipitation per time step back into the data struicture
            pfp_utils.CreateSeries(ds_all,output_label,precip,Flag=accum_flag,Attr=accum_attr)
        # calculate missing humidities
        RH_list = sorted([x for x in ds_all.series.keys() if ("RH" in x) and ("_QCFlag" not in x)])
        Ta_list = sorted([x for x in ds_all.series.keys() if ("Ta" in x) and ("_QCFlag" not in x)])
        ps_list = sorted([x for x in ds_all.series.keys() if ("ps" in x) and ("_QCFlag" not in x)])
        for RH_label,Ta_label,ps_label in zip(RH_list,Ta_list,ps_list):
            Ta,f,a = pfp_utils.GetSeriesasMA(ds_all,Ta_label)
            RH,f,a = pfp_utils.GetSeriesasMA(ds_all,RH_label)
            ps,f,a = pfp_utils.GetSeriesasMA(ds_all,ps_label)
            Ah = mf.absolutehumidityfromRH(Ta, RH)
            attr = pfp_utils.MakeAttributeDictionary(long_name='Absolute humidity',units='g/m3',standard_name='not defined',
                                                   bom_id=a["bom_id"],bom_name=a["bom_name"],bom_dist=a["bom_dist"])
            pfp_utils.CreateSeries(ds_all,RH_label.replace("RH","Ah"),Ah,Flag=f,Attr=attr)
            q = mf.specifichumidityfromRH(RH, Ta, ps)
            attr = pfp_utils.MakeAttributeDictionary(long_name='Specific humidity',units='kg/kg',standard_name='not defined',
                                                   bom_id=a["bom_id"],bom_name=a["bom_name"],bom_dist=a["bom_dist"])
            pfp_utils.CreateSeries(ds_all,RH_label.replace("RH","q"),q,Flag=f,Attr=attr)
        
        # now write the data structure to file
        # OMG, the user may want to overwrite the old data ...
        if os.path.exists(ncname):
            # ... but we will save them from themselves!
            fnames_list = os.listdir(site_out_path)
            suffix_date_dict = {}
            for fname in fnames_list:
                suffix = os.path.splitext(fname)[0].split('_')[-1]
                try:
                    date = datetime.datetime.strptime(suffix, "%Y%m%d%H%M")
                    suffix_date_dict[date] = os.path.join(site_out_path, fname)
                except:
                    continue
            remove_list = sorted(suffix_date_dict.keys())
            for date in remove_list[:-1]:
                os.remove(suffix_date_dict[date])
            t = time.localtime()
            rundatetime = datetime.datetime(t[0],t[1],t[2],t[3],t[4],t[5]).strftime("%Y%m%d%H%M")
            new_ext = "_"+rundatetime+".nc"
            # add the current local datetime the old file name
            newFileName = ncname.replace(".nc",new_ext)
            msg = " Renaming "+ncname+" to "+newFileName
            logging.info(msg)
            # ... and rename the old file to preserve it
            os.rename(ncname,newFileName)
            # now the old file will not be overwritten
        ncfile = pfp_io.nc_open_write(ncname)
        pfp_io.nc_write_series(ncfile,ds_all,ndims=1)
        logging.info("Finished site: "+site_name)
#------------------------------------------------------------------------------

###############################################################################
# Main code                                                                   #
###############################################################################

# Set up logging    
t = time.localtime()
rundatetime = datetime.datetime(t[0],t[1],t[2],t[3],t[4],t[5]).strftime("%Y%m%d%H%M")
log_filename = '/mnt/OzFlux/Logfiles/AWS/aws2nc_'+rundatetime+'.log'
logging.basicConfig(filename=log_filename,
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt = '%H:%M:%S',
                    level=logging.DEBUG)
console = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', '%H:%M:%S')
console.setFormatter(formatter)
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

# Basic configurations
in_path = "/rdsi/market/CloudStor/Shared/AWS_BOM_all"
out_path = "/mnt/OzFlux/Sites/"
master_file_pathname = "/mnt/OzFlux/Sites/site_master.xls"

try:
    aws_to_nc(in_path, out_path, master_file_pathname)
    logging.info('Downsampling to 1 hour time step for relevant sites...')
    downsample_aws(out_path, master_file_pathname)
    print "aws2nc: All done"
    msg = ('Successfully processed BOM data and wrote to site netCDF AWS files'
           '(see log for details)')
    grunt_email.email_send(mail_recipients, 'Site AWS netCDF write status', msg)
except Exception, e:
    msg = ('Data processing failed with the following message: {}; '
           '(see log for details)'.format(e))
    print msg
    grunt_email.email_send(mail_recipients, 'Site AWS netCDF write status', msg)
    
