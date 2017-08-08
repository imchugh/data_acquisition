# Import standard modules
import netCDF4
import numpy
import os
import pdb
import sys
import xlrd

# Import custom modules
scripts_dir = '../../../OzFluxQC/scripts'
# check the scripts directory is present
if not os.path.exists(scripts_dir):
    print "the scripts directory is missing"
    sys.exit()
# since the scripts directory is there, try importing the modules
sys.path.append(scripts_dir)
import qcio
import qcutils

###############################################################################
# Functions                                                                   #
###############################################################################

#------------------------------------------------------------------------------
def func(aws_name):
        
    ds_aws_30minute = qcio.nc_read_series(aws_name)
    has_gaps = qcutils.CheckTimeStep(ds_aws_30minute)
    if has_gaps:
        print "Problems found with time step"
        qcutils.FixTimeStep(ds_aws_30minute)
        qcutils.get_ymdhmsfromdatetime(ds_aws_30minute)
    dt_aws_30minute = ds_aws_30minute.series["DateTime"]["Data"]
    ddt=[dt_aws_30minute[i+1]-dt_aws_30minute[i] for i in range(0,len(dt_aws_30minute)-1)]
    print "Minimum time step is",min(ddt)," Maximum time step is",max(ddt)
    
    dt_aws_30minute = ds_aws_30minute.series["DateTime"]["Data"]
    start_date = dt_aws_30minute[0]
    end_date = dt_aws_30minute[-1]
    si_wholehour = qcutils.GetDateIndex(dt_aws_30minute,str(start_date),ts=30,match="startnexthour")
    ei_wholehour = qcutils.GetDateIndex(dt_aws_30minute,str(end_date),ts=30,match="endprevioushour")
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
        ds_aws_60minute = qcio.DataStructure()
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
        ds_aws_60minute.series["DateTime"]["Attr"] = qcutils.MakeAttributeDictionary(long_name="DateTime in local time zone",units="None")
        # add the Excel datetime, year, month etc
        qcutils.get_xldatefromdatetime(ds_aws_60minute)
        qcutils.get_ymdhmsfromdatetime(ds_aws_60minute)
        # loop over the series and take the average (every thing but Precip) or sum (Precip)
        for item in series_list:
            if "Precip" in item:
                data_30minute,flag_30minute,attr = qcutils.GetSeriesasMA(ds_aws_30minute,item,si=si_wholehour,ei=ei_wholehour)
                data_2d = numpy.reshape(data_30minute,(nRecs_30minute/2,2))
                flag_2d = numpy.reshape(flag_30minute,(nRecs_30minute/2,2))
                data_60minute = numpy.ma.sum(data_2d,axis=1)
                flag_60minute = numpy.ma.max(flag_2d,axis=1)
                qcutils.CreateSeries(ds_aws_60minute,item,data_60minute,Flag=flag_60minute,Attr=attr)
            elif "Wd" in item:
                Ws_30minute,flag_30minute,attr = qcutils.GetSeriesasMA(ds_aws_30minute,item,si=si_wholehour,ei=ei_wholehour)
                Wd_30minute,flag_30minute,attr = qcutils.GetSeriesasMA(ds_aws_30minute,item,si=si_wholehour,ei=ei_wholehour)
                U_30minute,V_30minute = qcutils.convert_WsWdtoUV(Ws_30minute,Wd_30minute)
                U_2d = numpy.reshape(U_30minute,(nRecs_30minute/2,2))
                V_2d = numpy.reshape(V_30minute,(nRecs_30minute/2,2))
                flag_2d = numpy.reshape(flag_30minute,(nRecs_30minute/2,2))
                U_60minute = numpy.ma.sum(U_2d,axis=1)
                V_60minute = numpy.ma.sum(V_2d,axis=1)
                Ws_60minute,Wd_60minute = qcutils.convert_UVtoWsWd(U_60minute,V_60minute)
                flag_60minute = numpy.ma.max(flag_2d,axis=1)
                qcutils.CreateSeries(ds_aws_60minute,item,Wd_60minute,Flag=flag_60minute,Attr=attr)
            else:
                data_30minute,flag_30minute,attr = qcutils.GetSeriesasMA(ds_aws_30minute,item,si=si_wholehour,ei=ei_wholehour)
                data_2d = numpy.reshape(data_30minute,(nRecs_30minute/2,2))
                flag_2d = numpy.reshape(flag_30minute,(nRecs_30minute/2,2))
                data_60minute = numpy.ma.average(data_2d,axis=1)
                flag_60minute = numpy.ma.max(flag_2d,axis=1)
                qcutils.CreateSeries(ds_aws_60minute,item,data_60minute,Flag=flag_60minute,Attr=attr)
        # write out the 60 minute data
        aws_30min_name = aws_name.replace('.nc','_30minute.nc')
        os.rename(aws_name, aws_30min_name) 
        ncfile = qcio.nc_open_write(aws_name)
        qcio.nc_write_series(ncfile, ds_aws_60minute, ndims=1)
#------------------------------------------------------------------------------

###############################################################################
# Main program                                                                #
###############################################################################
def main():
    path_to_master_file = '/mnt/OzFlux/site_master_file.xls'
    path_to_site_files = '/mnt/OzFlux/Sites/{}/Data/AWS'

    wb = xlrd.open_workbook(path_to_master_file)
    sheet = wb.sheet_by_name('OzFlux')
    site_list = sheet.col_values(0, 10)
    time_step_list = sheet.col_values(4, 10)
    hr_sites_list = [''.join(site_list[i].split(' ')) for i, time_step 
                     in enumerate(time_step_list) if time_step == 60]

    for site in hr_sites_list:

#    work_list = []
#    archive_dict = {}
#    path = path_to_site_files.format(site)
#    f_list = os.listdir(path)
#    for f in f_list:
#        fpath = os.path.join(path, f)
#        date = dt.datetime.fromtimestamp(os.path.getmtime(fpath))
#        f_suffix = os.path.splitext(f)[0].split('_')[-1]
#        if f_suffix == 'AWS':
#            current_f = f
#        elif 'minute' in f_suffix:
#            work_list.append(f)
#        else:
#            try:
#                int(f_suffix)
#                archive_dict[date] = f
#            except:
#                pass

#    oldest_f = archive_dict[sorted(archive_dict.keys())[0]]
#    newest_f = archive_dict[sorted(archive_dict.keys())[-1]]
#    print ('The oldest file is {0} and the newest file is {1}'
#           .format(oldest_f, newest_f))

        print 'Running timestep conversion for {} site:'.format(site)

        path = path_to_site_files.format(site)
        current_f = '{}_AWS.nc'.format(site)
        target = os.path.join(path, current_f)
        ncobj = netCDF4.Dataset(target)
        attr_dict = ncobj.__dict__
        if int(attr_dict['time_step']) == 60:
            print 'Conversion already complete!'
        elif int(attr_dict['time_step']) == 30:
            print 'Conversion required! Starting now...'
            func(target)
            print 'Conversion complete!'

###############################################################################
