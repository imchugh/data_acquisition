# Import standard modules
import sys
sys.path.append('/home/ian/OzFlux/OzFluxQC/scripts')
import csv
import datetime
import glob
import logging
import netCDF4
import numpy
import os
import time
import xlrd
import pdb
import ftplib
import StringIO
import zipfile

# Import OzFluxQC modules
import constants as c
import meteorologicalfunctions as mf
import qcio
import qcts
import qcutils

###############################################################################
# Functions                                                                   #
###############################################################################

#------------------------------------------------------------------------------
# Grab the bom data from the ftp server
def get_ftp_data(ftp_server, ftp_dir, output_dir, req_file_list):

    # Login to ftp server         
    ftp = ftplib.FTP(ftp_server)
    ftp.login()   
    
    # Check directories exist and make them if not
    if not os.path.isdir(output_dir): os.mkdir(output_dir)
        
    # Open the zip files and unzip to directory - ignore the solar data
    master_file_list = []
    zip_file_list = [os.path.split(f)[1] for f in ftp.nlst(ftp_dir)]   
    for this_file in zip_file_list:
        if 'globalsolar' in this_file: continue
        in_file = os.path.join(ftp_dir, this_file)
        f_str = 'RETR {0}'.format(in_file)
        sio = StringIO.StringIO()
        ftp.retrbinary(f_str, sio.write)
        sio.seek(0)
        zip_obj = zipfile.ZipFile(sio)
        file_list = zip_obj.namelist()
        file_list = subset_station_list(file_list, req_file_list)
        master_file_list = master_file_list + file_list
        for f in file_list:
            if not os.path.isfile(os.path.join(output_dir, f)):
                zip_obj.extract(f, output_dir)
        zip_obj.close()
    
    # Check for differences between requested site files and available site
    # files and report to user (print to screen)
    returned_file_list = [f.split('_')[2] for f in master_file_list]
    missing_file_list = list(set(req_file_list) - set(returned_file_list))
    print ('The following site IDs were not available: {0}'
           .format(', '.join(missing_file_list)))
       
    ftp.close()

    return
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# get the site information and the AWS stations to use
def get_bom_site_details(path_to_file, sheet_name):

    wb = xlrd.open_workbook(path_to_file)
    sheet = wb.sheet_by_name(sheet_name)
    xl_row = 10
    bom_sites_info = {}
    for row in range(xl_row,sheet.nrows):
        xlrow = sheet.row_values(row)
        flux_site_name = str(xlrow[0])
        bom_sites_info[flux_site_name] = {}
        for i, var in enumerate(['latitude', 'longitude', 'elevation']):
            bom_sites_info[flux_site_name][var] = xlrow[i + 1]
        for col_idx in [4, 10, 16, 22]:
            try:
                bom_site_name = xlrow[col_idx]
                bom_id = str(int(xlrow[col_idx + 1])).zfill(6)
                bom_sites_info[flux_site_name][bom_id] = {'site_name': 
                                                          bom_site_name}
                for i, var in enumerate(['latitude', 'longitude', 'elevation', 
                                         'distance']):
                    bom_sites_info[flux_site_name][bom_id][var] = (
                        xlrow[col_idx + i + 2])
            except:
                continue
    
    return bom_sites_info
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# Strip a sorted list from the sites info file
def get_bom_id_list(bom_sites_info):
    
    bom_id_list = []
    for key in bom_sites_info.keys():
        for sub_key in bom_sites_info[key].keys():
            try:
                int(sub_key)
                bom_id_list.append(sub_key)
            except:
                continue
    
    return sorted(bom_id_list)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def subset_station_list(files_list, target_ID_list):
    
    unq_files_list = sorted(list(set(files_list)))
    unq_target_ID_list = sorted(list(set(target_ID_list)))
    f_names_list = []
    counter = 0
    for ID in unq_target_ID_list:
        for f_name in unq_files_list[counter:]:
            if ID in f_name:
                f_names_list.append(f_name)
                counter = unq_files_list.index(f_name)
                break
                
    return f_names_list
#------------------------------------------------------------------------------

###############################################################################
# Main program                                                                #
###############################################################################

# Set stuff up
ftp_server = 'ftp.bom.gov.au'
ftp_dir = 'anon2/home/ncc/srds/Scheduled_Jobs/DS010_UWA-SEE/'
output_dir = '/home/ian/BOM_data'
xlname = "/home/ian/Temp/AWS_Locations.xls"

# Set up logging
t = time.localtime()
rundatetime = datetime.datetime(t[0],t[1],t[2],t[3],t[4],t[5]).strftime("%Y%m%d%H%M")
log_filename = '/home/ian/Temp/logfiles/aws2nc_'+rundatetime+'.log'    
logging.basicConfig(filename=log_filename,
                    format='%(asctime)s %(levelname)s %(message)s',
                    datefmt = '%H:%M:%S',
                    level=logging.DEBUG)
console = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', '%H:%M:%S')
console.setFormatter(formatter)
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

# dummy control file for FixTimeSteps
cf = {"Options":{"FixTimeStepMethod":"round"}}

# get bom site details
bom_sites_info = get_bom_site_details(xlname, 'OzFlux')
bom_id_list = get_bom_id_list(bom_sites_info)
get_ftp_data(ftp_server, ftp_dir, output_dir, bom_id_list)

#in_path = "/mnt/OzFlux/AWS/Current/"



#
#in_path = "/mnt/OzFlux/AWS/Current/"
#out_path = "/mnt/OzFlux/Sites/"
#in_filename = in_path+"HM01X_Data*.csv"
#file_list = sorted(glob.glob(in_filename))
#
#site_list = bom_sites_info.keys()
##site_list = ["Tumbarumba"]
##site_list = ["Great Western Woodlands"]
##site_list = ["Otway"]
#for site_name in sorted(site_list):
#    logging.info("Starting site: "+site_name)
#    sname = site_name.replace(" ","")
#    ncname = out_path + sname + "/Data/AWS/" + sname + "_AWS.nc"
##    print ncname
##    pdb.set_trace()
##    ncname = os.path.join(out_path,sname,"/Data/AWS/",sname+"_AWS.nc")
#    site_latitude = bom_sites_info[site_name]["latitude"]
#    site_longitude = bom_sites_info[site_name]["longitude"]
#    site_elevation = bom_sites_info[site_name]["elevation"]
#    site_number_list = bom_sites_info[site_name].keys()
#    for item in ["latitude","longitude","elevation"]:
#        if item in site_number_list: site_number_list.remove(item)
#    # read the CSV files and put the contents into data_dict
#    data_dict = {}
#    for idx,sn in enumerate(site_number_list):
#        # get a list of file names that contain the relevent station numbers
#        csvname = [fn for fn in file_list if str(sn) in fn]
#        # continue to next station if this station not in file_list
#        if len(csvname)==0: continue
#        logging.info("Reading CSV file: "+str(csvname[0]))
#        # columns are:
#        # file data content
#        #  1    0    station number
#        #  7    1    year, local standard time
#        #  8    2    month, local standard time
#        #  9    3    day, local standard time
#        #  10   4    hour, local standard time
#        #  11   5    minute, local standard time
#        #  12   6    precip since 0900
#        #  14   7    air temperature, C
#        #  16   8    dew point temperature, C
#        #  18   9    relative humidity, %
#        #  20   10   wind speed, m/s
#        #  22   11   wind direction, degT
#        #  24   12   gust in last 10 minutes, m/s
#        #  26   13   station pressure, hPa
#        data=numpy.genfromtxt(csvname[0],skip_header=1,delimiter=",",usecols=(1,7,8,9,10,11,12,14,16,18,20,22,24,26),
#                              missing_values=-9999,filling_values=-9999)
#        data = numpy.ma.masked_equal(data,float(-9999),copy=True)
#        data_dict[sn] = data
#    # now pull the data out and put it in separate data structures, one per station, all
#    # of which are held in a data structure dictionary
#    ds_dict = {}
#    for bom_id in data_dict.keys():
#        logging.info("Processing BoM station: "+str(bom_id))
#        # create a data structure
#        ds=qcio.DataStructure()
#        # put the year, month, day, hour and minute into the data structure
#        nRecs = data_dict[bom_id].shape[0]
#        ds.globalattributes["nc_nrecs"] = nRecs
#        ds.globalattributes["time_step"] = 30
#        ds.globalattributes["latitude"] = bom_sites_info[site_name][str(bom_id)]["latitude"]
#        ds.globalattributes["longitude"] = bom_sites_info[site_name][str(bom_id)]["longitude"]
#        flag = numpy.zeros(nRecs,dtype=numpy.int32)
#        Seconds = numpy.zeros(nRecs,dtype=numpy.float64)
#        qcutils.CreateSeries(ds,'Year',data_dict[bom_id][:,1],Flag=flag,Attr=qcutils.MakeAttributeDictionary(long_name='Year',units='none'))
#        qcutils.CreateSeries(ds,'Month',data_dict[bom_id][:,2],Flag=flag,Attr=qcutils.MakeAttributeDictionary(long_name='Month',units='none'))
#        qcutils.CreateSeries(ds,'Day',data_dict[bom_id][:,3],Flag=flag,Attr=qcutils.MakeAttributeDictionary(long_name='Day',units='none'))
#        qcutils.CreateSeries(ds,'Hour',data_dict[bom_id][:,4],Flag=flag,Attr=qcutils.MakeAttributeDictionary(long_name='Hour',units='none'))
#        qcutils.CreateSeries(ds,'Minute',data_dict[bom_id][:,5],Flag=flag,Attr=qcutils.MakeAttributeDictionary(long_name='Minute',units='none'))
#        qcutils.CreateSeries(ds,'Second',Seconds,Flag=flag,Attr=qcutils.MakeAttributeDictionary(long_name='Second',units='none'))
#        # now get the Python datetime
#        qcutils.get_datetimefromymdhms(ds)
#        # now put the data into the data structure
#        attr=qcutils.MakeAttributeDictionary(long_name='Precipitation since 0900',units='mm',
#                                             bom_id=str(bom_id),bom_name=bom_sites_info[site_name][str(bom_id)]["site_name"],
#                                             bom_dist=bom_sites_info[site_name][str(bom_id)]["distance"])
#        qcutils.CreateSeries(ds,'Precip',data_dict[bom_id][:,6],Flag=flag,Attr=attr)
#        attr=qcutils.MakeAttributeDictionary(long_name='Air temperature',units='C',
#                                             bom_id=str(bom_id),bom_name=bom_sites_info[site_name][str(bom_id)]["site_name"],
#                                             bom_dist=bom_sites_info[site_name][str(bom_id)]["distance"])
#        qcutils.CreateSeries(ds,'Ta',data_dict[bom_id][:,7],Flag=flag,Attr=attr)
#        attr=qcutils.MakeAttributeDictionary(long_name='Dew point temperature',units='C',
#                                             bom_id=str(bom_id),bom_name=bom_sites_info[site_name][str(bom_id)]["site_name"],
#                                             bom_dist=bom_sites_info[site_name][str(bom_id)]["distance"])
#        qcutils.CreateSeries(ds,'Td',data_dict[bom_id][:,8],Flag=flag,Attr=attr)
#        attr=qcutils.MakeAttributeDictionary(long_name='Relative humidity',units='%',
#                                             bom_id=str(bom_id),bom_name=bom_sites_info[site_name][str(bom_id)]["site_name"],
#                                             bom_dist=bom_sites_info[site_name][str(bom_id)]["distance"])
#        qcutils.CreateSeries(ds,'RH',data_dict[bom_id][:,9],Flag=flag,Attr=attr)
#        attr=qcutils.MakeAttributeDictionary(long_name='Wind speed',units='m/s',
#                                             bom_id=str(bom_id),bom_name=bom_sites_info[site_name][str(bom_id)]["site_name"],
#                                             bom_dist=bom_sites_info[site_name][str(bom_id)]["distance"])
#        qcutils.CreateSeries(ds,'Ws',data_dict[bom_id][:,10],Flag=flag,Attr=attr)
#        attr=qcutils.MakeAttributeDictionary(long_name='Wind direction',units='degT',
#                                             bom_id=str(bom_id),bom_name=bom_sites_info[site_name][str(bom_id)]["site_name"],
#                                             bom_dist=bom_sites_info[site_name][str(bom_id)]["distance"])
#        qcutils.CreateSeries(ds,'Wd',data_dict[bom_id][:,11],Flag=flag,Attr=attr)
#        attr=qcutils.MakeAttributeDictionary(long_name='Wind gust',units='m/s',
#                                             bom_id=str(bom_id),bom_name=bom_sites_info[site_name][str(bom_id)]["site_name"],
#                                             bom_dist=bom_sites_info[site_name][str(bom_id)]["distance"])
#        qcutils.CreateSeries(ds,'Wg',data_dict[bom_id][:,12],Flag=flag,Attr=attr)
#        data_dict[bom_id][:,13] = data_dict[bom_id][:,13]/float(10)
#        attr=qcutils.MakeAttributeDictionary(long_name='Air Pressure',units='kPa',
#                                             bom_id=str(bom_id),bom_name=bom_sites_info[site_name][str(bom_id)]["site_name"],
#                                             bom_dist=bom_sites_info[site_name][str(bom_id)]["distance"])
#        qcutils.CreateSeries(ds,'ps',data_dict[bom_id][:,13],Flag=flag,Attr=attr)
#        # fix any time stamp issues
#        if qcutils.CheckTimeStep(ds):
#            qcutils.FixTimeStep(ds)
#            # update the Year, Month, Day etc from the Python datetime
#            qcutils.get_ymdhmsfromdatetime(ds)
#        # now interpolate
#        for label in ["Precip","Ta","Td","RH","Ws","Wd","Wg","ps"]:
#            qcts.InterpolateOverMissing(ds,series=label,maxlen=2)
#        # put this stations data into the data structure dictionary
#        ds_dict[bom_id] = ds
#
#    # get the earliest start datetime and the latest end datetime
#    logging.info("Finding the start and end dates")
#    bom_id_list = ds_dict.keys()
#    ds0 = ds_dict[bom_id_list[0]]
#    ldt = ds0.series["DateTime"]["Data"]
#    #print bom_id_list[0],":",ldt[0],ldt[-1]
#    start_date = ldt[0]
#    end_date = ldt[-1]
#    bom_id_list.remove(bom_id_list[0])
#    for bom_id in bom_id_list:
#        dsn = ds_dict[bom_id]
#        ldtn = dsn.series["DateTime"]["Data"]
#        #print bom_id,":",ldtn[0],ldtn[-1]
#        start_date = min([start_date,ldtn[0]])
#        end_date = max([end_date,ldtn[-1]])
#    #print start_date,end_date
#
#    # merge the individual data structures into a single one
#    logging.info("Merging file contents")
#    ds_all = qcio.DataStructure()
#    ds_all.globalattributes["time_step"] = 30
#    ds_all.globalattributes["xl_datemode"] = 0
#    ds_all.globalattributes["site_name"] = site_name
#    ds_all.globalattributes["latitude"] = site_latitude
#    ds_all.globalattributes["longitude"] = site_longitude
#    ds_all.globalattributes["elevation"] = site_elevation
#    ts = int(ds_all.globalattributes["time_step"])
#    ldt_all = [result for result in qcutils.perdelta(start_date,end_date,datetime.timedelta(minutes=ts))]
#    nRecs = len(ldt_all)
#    ds_all.globalattributes["nc_nrecs"] = nRecs
#    ds_all.series["DateTime"] = {}
#    ds_all.series["DateTime"]["Data"] = ldt_all
#    flag = numpy.zeros(nRecs,dtype=numpy.int32)
#    ds_all.series["DateTime"]["Flag"] = flag
#    ds_all.series["DateTime"]["Attr"] = {}
#    ds_all.series['DateTime']["Attr"]["long_name"] = "Date-time object"
#    ds_all.series['DateTime']["Attr"]["units"] = "None"
#    # get the year, month, day, hour, minute and seconds from the Python datetime
#    qcutils.get_ymdhmsfromdatetime(ds_all)
#    # get the xlDateTime from the 
#    xlDateTime = qcutils.get_xldatefromdatetime(ds_all)
#    attr = qcutils.MakeAttributeDictionary(long_name="Date/time in Excel format",units="days since 1899-12-31 00:00:00")
#    qcutils.CreateSeries(ds_all,"xlDateTime",xlDateTime,Flag=flag,Attr=attr)
#    # loop over the stations
#    for idx,bom_id in enumerate(ds_dict.keys()):
#        logging.info("Merging BoM site: "+str(bom_id))
#        ds = ds_dict[bom_id]
#        ldt = ds.series["DateTime"]["Data"]
#        index = qcutils.FindIndicesOfBInA(ldt,ldt_all)
#        # loop over the variables
#        for label in ["Precip","Ta","Td","RH","Ws","Wd","Wg","ps"]:
#            data_all = numpy.ma.ones(nRecs,dtype=numpy.float64)*float(c.missing_value)
#            flag_all = numpy.zeros(nRecs,dtype=numpy.int32)
#            data,flag,attr = qcutils.GetSeriesasMA(ds,label)
#            data_all[index] = data
#            flag_all[index] = flag
#            output_label = label+"_"+str(idx)
#            attr["bom_id"] = str(bom_id)
#            qcutils.CreateSeries(ds_all,output_label,data_all,Flag=flag_all,Attr=attr)
#    # get precipitation per time step
#    # now get precipitation per time step from the interpolated precipitation accumulated over the day
#    precip_list = [x for x in ds_all.series.keys() if ("Precip" in x) and ("_QCFlag" not in x)]
#    #print precip_list
#    logging.info("Converting 24 hour accumulated precipitation")
#    for output_label in precip_list:
#        # getthe accumlated precipitation
#        accum_data,accum_flag,accum_attr = qcutils.GetSeriesasMA(ds_all,output_label)
#        # make the flag a masked array
#        accum_flag = numpy.ma.array(accum_flag)
#        # round small precipitations to 0
#        index = numpy.ma.where(accum_data<0.01)[0]
#        accum_data[index] = float(0)
#        # get the precipitation per time step
#        precip = numpy.ma.ediff1d(accum_data,to_begin=0)
#        # trap the times when the accumlated precipitation is reset
#        # This should be at a standard time every day for all BoM sites but this is not the case
#        # For eaxample, at 72161, there is a period in 09/2010 when the reset seems to occur at
#        # 1000 instead of the expected 0900 (possible daylight saving error?)
#        # To get around this, we check the differentiated precipitation:
#        # - if the precipitation per time step is positive, do nothing
#        # - if the precipitation per time step is negative;
#        #   - if the QC flag is 50 (linear interpolation) then set the precipitation to 0
#        #   - if the QC flag is 0 then set the precipitation per time step to the accumlated
#        #     precipitation
#        # find times when the precipitation per time step is negative and the QC flag is not 0 ...
#        index = numpy.ma.where((precip<0)&(accum_flag!=0))[0]
#        # ... and set the precipitation per time step for these times to 0
#        precip[index] = float(0)
#        # find any remaining times when the precipitation per time step is negative ...
#        index = numpy.ma.where(precip<0)[0]
#        # ... and set them to the accumulated precipitation
#        precip[index] = accum_data[index]
#        #index = [x for x in range(len(ldt_all)) if (ldt_all[x].hour==8) and (ldt_all[x].minute==30)]
#        #precip[index] = float(0)
#        #index = [x for x in range(len(ldt_all)) if (ldt_all[x].hour==9) and (ldt_all[x].minute==0)]
#        #precip[index] = accum_24hr[index]
#        # set attributes as appropriate
#        accum_attr["long_name"] = "Precipitation total over time step"
#        accum_attr["units"] = "mm/30 minutes"
#        # put the precipitation per time step back into the data struicture
#        qcutils.CreateSeries(ds_all,output_label,precip,Flag=accum_flag,Attr=accum_attr)
#    # calculate missing humidities
#    RH_list = sorted([x for x in ds_all.series.keys() if ("RH" in x) and ("_QCFlag" not in x)])
#    Ta_list = sorted([x for x in ds_all.series.keys() if ("Ta" in x) and ("_QCFlag" not in x)])
#    ps_list = sorted([x for x in ds_all.series.keys() if ("ps" in x) and ("_QCFlag" not in x)])
#    for RH_label,Ta_label,ps_label in zip(RH_list,Ta_list,ps_list):
#        Ta,f,a = qcutils.GetSeriesasMA(ds_all,Ta_label)
#        RH,f,a = qcutils.GetSeriesasMA(ds_all,RH_label)
#        ps,f,a = qcutils.GetSeriesasMA(ds_all,ps_label)
#        Ah = mf.absolutehumidityfromRH(Ta, RH)
#        attr = qcutils.MakeAttributeDictionary(long_name='Absolute humidity',units='g/m3',standard_name='not defined',
#                                               bom_id=a["bom_id"],bom_name=a["bom_name"],bom_dist=a["bom_dist"])
#        qcutils.CreateSeries(ds_all,RH_label.replace("RH","Ah"),Ah,Flag=f,Attr=attr)
#        q = mf.specifichumidityfromRH(RH, Ta, ps)
#        attr = qcutils.MakeAttributeDictionary(long_name='Specific humidity',units='kg/kg',standard_name='not defined',
#                                               bom_id=a["bom_id"],bom_name=a["bom_name"],bom_dist=a["bom_dist"])
#        qcutils.CreateSeries(ds_all,RH_label.replace("RH","q"),q,Flag=f,Attr=attr)
#    
#    # now write the data structure to file
#    # OMG, the user may want to overwrite the old data ...
#    if os.path.exists(ncname):
#        # ... but we will save them from themselves!
#        t = time.localtime()
#        rundatetime = datetime.datetime(t[0],t[1],t[2],t[3],t[4],t[5]).strftime("%Y%m%d%H%M")
#        new_ext = "_"+rundatetime+".nc"
#        # add the current local datetime the old file name
#        newFileName = ncname.replace(".nc",new_ext)
#        msg = " Renaming "+ncname+" to "+newFileName
#        logging.info(msg)
#        # ... and rename the old file to preserve it
#        os.rename(ncname,newFileName)
#        # now the old file will not be overwritten
##    pdb.set_trace()
#    ncfile = qcio.nc_open_write(ncname)
#    qcio.nc_write_series(ncfile,ds_all,ndims=1)
#    logging.info("Finished site: "+site_name)
#
#print "aws2nc: All done"