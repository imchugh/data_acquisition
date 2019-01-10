#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 17 14:59:15 2018

@author: ian
"""

import datetime as dt
import netCDF4
import numpy
import operator
import os
import pandas as pd
import pdb
from pytz import timezone
from pytz.exceptions import NonExistentTimeError, AmbiguousTimeError
import sys
from timezonefinder import TimezoneFinder as tzf
import xlrd

# PFP
#sys.path.append('/home/ian/OzFlux/PyFluxPro/scripts')
#import constants as c
#import meteorologicalfunctions as mf
#import pfp_io
#import pfp_utils

#------------------------------------------------------------------------------
class ACCESSData(object):
    def __init__(self):
        self.globalattr = {}
        self.variables = {}
        self.varattr = {}
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def access_read_mfiles2(path):

    # Initialise
    var_dict = makevardict()
    var_list = var_dict.keys()
    f = ACCESSData()

    # open the netCDF file
    ncfile = netCDF4.Dataset(path)

    # check the record structure integrity
    dims = ncfile.dimensions
    shape = (len(dims["time"]),len(dims["lat"]),len(dims["lon"]))
    if shape[1]!=3:
        print ('length of lat dimension in {0} is {1} (expected 3)'
               .format(os.path.basename(path), str(shape[1])))
        return
    if shape[2]!=3:
        print ('length of lon dimension in {0} is {1} (expected 3)'
               .format(os.path.basename(path), str(shape[2])))
        return
    
#    # get the python datetime and convert from utc
#    base_date_str = getattr(ncfile.variables['time'], 'units').split(' ')[2:4]
#    base_date = dt.datetime.strptime(''.join(base_date_str), '%Y-%m-%d%H:%M:%S')
#    hours = ncfile.variables['time'][:].data * 24
#    utc_list = []
#    local_list = []
#    mask = []
#    for hour in hours:
#        try:
#            dt_list.append(base_date + dt.timedelta(hours = hour))
#        except:
#            dt_list.append(-9999)
#    f.variables['DateTime_UTC'] = 
#    local_time = convert_utc([base_date_time], df.Latitude, df.Longitude, 
#                             'from_utc')[0]
#    temp_dict['date_time_local'] = local_time
    
    # get the global attributes
    for gattr in ncfile.ncattrs():
        f.globalattr[gattr] = getattr(ncfile,gattr)
            
    # load the data into the data structure
    for var in var_list:
        access_name = var_dict[var]
        try:
            assert access_name in ncfile.variables.keys()
            f.variables[access_name] = ncfile.variables[access_name][:]
            f.varattr[access_name] = {}
            for this_attr in ncfile.variables[access_name].ncattrs():
                f.varattr[access_name][this_attr] = getattr(ncfile.variables[access_name], 
                                                            this_attr)
        except AssertionError:
            f.variables[access_name] = makedummyseries(shape)

    ncfile.close()

    return f
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def convert_utc(date, lat, lon, direction):
    """Convert a python datetime to or from utc"""

    ops = {'+': operator.add, '-': operator.sub}
    if not direction in ['from_utc', 'to_utc']:
        raise KeyError('direction parameter must be either to_utc or from_utc')
    if not isinstance(date, dt.datetime):
        raise TypeError('a valid python datetime must be passed as first arg')
    op = ops['-'] if direction == 'to_utc' else ops['+']
    tz = timezone(tzf().timezone_at(lat = lat, lng = lon))
    l = []
    try:
        offset = tz.utcoffset(date) - tz.dst(date)
    except NonExistentTimeError:
        trim_date = date - dt.timedelta(seconds = 3600)
        offset = tz.utcoffset(trim_date) - tz.dst(trim_date)
    except AmbiguousTimeError:
        trim_date = date + dt.timedelta(seconds = 3600)
        offset = tz.utcoffset(trim_date) - tz.dst(trim_date)
    return op(date, offset)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_accessdata(ds_60minutes,f,info):

    # list of variables to process
    var_dict = makevardict()
    var_list = var_dict.keys()

    # latitude and longitude, chose central pixel of 3x3 grid
    ds_60minutes.globalattributes["latitude"] = f.variables["lat"][1]
    ds_60minutes.globalattributes["longitude"] = f.variables["lon"][1]

#    # get a series of Python datetimes and put this into the data structure
#    valid_date = f.variables["valid_date"][:]
#    nRecs = len(valid_date)
#    valid_time = f.variables["valid_time"][:]
#    dl = [datetime.datetime.strptime(str(int(valid_date[i])*10000+int(valid_time[i])),"%Y%m%d%H%M") for i in range(0,nRecs)]
#    dt_utc_all = numpy.array(dl)
#    time_step = numpy.array([(dt_utc_all[i]-dt_utc_all[i-1]).total_seconds() for i in range(1,len(dt_utc_all))])
#    time_step = numpy.append(time_step,3600)
#    idxne0 = numpy.where(time_step!=0)[0]
#    idxeq0 = numpy.where(time_step==0)[0]
#    idx_clipped = numpy.where((idxeq0>0)&(idxeq0<nRecs))[0]
#    idxeq0 = idxeq0[idx_clipped]
#    dt_utc = dt_utc_all[idxne0]
#    dt_utc = [x.replace(tzinfo=pytz.utc) for x in dt_utc]
#    dt_loc = [x.astimezone(info["site_tz"]) for x in dt_utc]
#    dt_loc = [x-x.dst() for x in dt_loc]
#    dt_loc = [x.replace(tzinfo=None) for x in dt_loc]
#    flag = numpy.zeros(len(dt_loc),dtype=numpy.int32)
#    ds_60minutes.series["DateTime"] = {}
#    ds_60minutes.series["DateTime"]["Data"] = dt_loc
#    ds_60minutes.series["DateTime"]["Flag"] = flag
#    ds_60minutes.series["DateTime_UTC"] = {}
#    ds_60minutes.series["DateTime_UTC"]["Data"] = dt_utc
#    ds_60minutes.series["DateTime_UTC"]["Flag"] = flag
#    nRecs = len(ds_60minutes.series["DateTime"]["Data"])
#    ds_60minutes.globalattributes["nc_nrecs"] = nRecs
#    # we're done with valid_date and valid_time, drop them from the variable list
#    for item in ["valid_date","valid_time","lat","lon"]:
#        if item in var_list: var_list.remove(item)
#    # create the QC flag with all zeros
#    nRecs = ds_60minutes.globalattributes["nc_nrecs"]
#    flag_60minutes = numpy.zeros(nRecs,dtype=numpy.int32)
#    # get the UTC hour
#    hr_utc = [x.hour for x in dt_utc]
#    attr = pfp_utils.MakeAttributeDictionary(long_name='UTC hour')
#    pfp_utils.CreateSeries(ds_60minutes,'Hr_UTC',hr_utc,Flag=flag_60minutes,Attr=attr)
#    # now loop over the variables listed in the control file
#    for label in var_list:
#        # get the name of the variable in the ACCESS file
#        access_name = var_dict[label]#pfp_utils.get_keyvaluefromcf(cf,["Variables",label],"access_name",default=label)
#        # warn the user if the variable not found
#        if access_name not in f.variables.keys():
#            msg = "Requested variable "+access_name
#            msg = msg+" not found in ACCESS data"
#            logging.error(msg)
#            continue
#        # get the variable attibutes
#        attr = get_variableattributes(f,access_name)
#        # loop over the 3x3 matrix of ACCESS grid data supplied
#        for i in range(0,3):
#            for j in range(0,3):
#                label_ij = label+'_'+str(i)+str(j)
#                if len(f.variables[access_name].shape)==3:
#                    series = f.variables[access_name][:,i,j]
#                elif len(f.variables[access_name].shape)==4:
#                    series = f.variables[access_name][:,0,i,j]
#                else:
#                    msg = "Unrecognised variable ("+label
#                    msg = msg+") dimension in ACCESS file"
#                    logging.error(msg)
#                series = series[idxne0]
#                pfp_utils.CreateSeries(ds_60minutes,label_ij,series,
#                                     Flag=flag_60minutes,Attr=attr)
#    return
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_input_data_path(site_name):
    pass
#------------------------------------------------------------------------------
    
#------------------------------------------------------------------------------
def get_ozflux_site_list(master_file_path):
    """Create a dataframe containing site names (index) and lat, long,  
       measurement interval and time zone"""
    
    wb = xlrd.open_workbook(master_file_path)
    sheet = wb.sheet_by_name('Active')
    header_row = 9
    header_list = sheet.row_values(header_row)
    df = pd.DataFrame()
    for var in ['Site', 'Latitude', 'Longitude', 'Time step']:
        alias = 'Time_step' if var == 'Time step' else var
        index_val = header_list.index(var)
        df[alias] = sheet.col_values(index_val, header_row + 1)   
    df.index = map(lambda x: '_'.join(x.split(' ')), df.Site)
    df.drop(header_list[0], axis = 1, inplace = True)
    df['Time_zone'] = map(lambda x: tzf().timezone_at(lat=x[0], lng=x[1]), 
                          zip(df.Latitude.tolist(), df.Longitude.tolist()))
    df['input_path'] = map(lambda x: os.path.join(input_data_path, 
                           '{}.nc'.format(x)), df.index)
    df['output_path'] = map(lambda x: os.path.join(output_data_path, 
                            '{}.nc'.format(x)), df.index)
    return df

#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def makedummyseries(shape):
    return numpy.ma.masked_all(shape)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def makevardict():

    # ACCESS variable name alias dictionary
    return {'Fsd': 'av_swsfcdown',
            'Fn_sw': 'av_netswsfc',
            'Fld': 'av_lwsfcdown',
            'Fn_lw': 'av_netlwsfc',
            'Ta': 'temp_scrn',
            'q': 'qsair_scrn',
            'Sws': 'soil_mois',
            'Ts': 'soil_temp',
            'u': 'u10',
            'v': 'v10',
            'ps': 'sfc_pres',
            'Precip': 'accum_prcp',
            'Fh': 'sens_hflx',
            'Fe': 'lat_hflx',
            'Habl': 'abl_ht',
            'lat': 'lat',
            'lon': 'lon',
            'time': 'time'}
#------------------------------------------------------------------------------

master_file_path = '/home/ian/Temp/site_master.xls'
input_data_path = '/home/ian/Temp/access_nc/201901'
output_data_path = '/home/ian/Temp/pyflux_nc/201901'

sites_df = get_ozflux_site_list(master_file_path)