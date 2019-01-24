#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Dec 17 14:59:15 2018
to do:
    Check what is happening with soil temperature depths treatment
    Add logger
@author: ian
"""

import datetime as dt
import netCDF4
import numpy
import os
import pandas as pd
import pdb
import pytz
from pytz import timezone
from scipy.interpolate import interp1d
import sys
from timezonefinder import TimezoneFinder as tzf
import xlrd

# PFP
sys.path.append('/home/ian/OzFlux/PyFluxPro/scripts')
import constants as c
import meteorologicalfunctions as mf
import pfp_io
import pfp_utils

#------------------------------------------------------------------------------
class ACCESSData(object):
    def __init__(self):
        self.globalattr = {}
        self.variables = {}
        self.varattr = {}
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def accumulate_rainfall(ds, label_suffix):
    
    pass
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def changeunits_airtemperature(ds, label_suffix):
    
    label = 'Ta' + label_suffix
    attr = pfp_utils.GetAttributeDictionary(ds,"Ta_00")
    if attr["units"] == "K":
        Ta,f,a = pfp_utils.GetSeriesasMA(ds, label)
        Ta = Ta - c.C2K
        attr["units"] = "C"
        pfp_utils.CreateSeries(ds, label, Ta, Flag = f, Attr = attr)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def changeunits_pressure(ds, label_suffix):
    
    label = 'ps' + label_suffix
    attr = pfp_utils.GetAttributeDictionary(ds,"ps_00")
    if attr["units"] == "Pa":
        ps,f,a = pfp_utils.GetSeriesasMA(ds, label)
        ps = ps/float(1000)
        attr["units"] = "kPa"
        pfp_utils.CreateSeries(ds, label, ps, Flag = f, Attr = attr)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def changeunits_soilmoisture(ds, label_suffix):

    label = "Sws" + label_suffix
    attr = pfp_utils.GetAttributeDictionary(ds, "Sws_00")
    Sws,f,a = pfp_utils.GetSeriesasMA(ds, label)
    Sws = Sws / float(100)
    attr["units"] = "frac"
    pfp_utils.CreateSeries(ds, label, Sws, Flag = f, Attr = attr)
    return
#------------------------------------------------------------------------------                

#------------------------------------------------------------------------------
def changeunits_soiltemperature(ds, label_suffix):
    
    label = 'Ts' + label_suffix
    attr = pfp_utils.GetAttributeDictionary(ds,"Ts_00")
    if attr["units"] == "K":
        Ts,f,a = pfp_utils.GetSeriesasMA(ds, label)
        Ts = Ts - c.C2K
        attr["units"] = "C"
        pfp_utils.CreateSeries(ds, label, Ts, Flag = f, Attr = attr)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def do_unit_conversions(ds):
    '''Convert original units from ACCESS netCDF into desired'''
    
    for i in range(0, 3):
        for j in range(0, 3):
            label_suffix = '_{0}{1}'.format(str(i), str(j))
            changeunits_airtemperature(ds, label_suffix)
            changeunits_soiltemperature(ds, label_suffix)
            changeunits_pressure(ds, label_suffix)
            get_windspeedanddirection(ds, label_suffix)
            get_relativehumidity(ds, label_suffix) # RH from T, spec hum + P
            get_absolutehumidity(ds, label_suffix) # AH from T and RH
            changeunits_soilmoisture(ds, label_suffix)
            get_radiation(ds, label_suffix)
            get_groundheatflux(ds, label_suffix) # as residual
            get_availableenergy(ds, label_suffix)
            get_regular_cuml_precip(ds, label_suffix)
#------------------------------------------------------------------------------
    
#------------------------------------------------------------------------------
def get_absolutehumidity(ds, label_suffix):

    Ta_label = "Ta" + label_suffix
    RH_label = "RH" + label_suffix
    Ah_label = "Ah" + label_suffix
    Ta,f,a = pfp_utils.GetSeriesasMA(ds, Ta_label)
    RH,f,a = pfp_utils.GetSeriesasMA(ds, RH_label)
    Ah = mf.absolutehumidityfromRH(Ta, RH)
    attr = pfp_utils.MakeAttributeDictionary(long_name = 'Absolute humidity',
                                             units = 'g/m3',
                                             standard_name = 'not defined')
    pfp_utils.CreateSeries(ds, Ah_label, Ah, Flag = f, Attr = attr)
    return
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_availableenergy(ds, label_suffix):

    label_Fg = "Fg" + label_suffix
    label_Fn = "Fn" + label_suffix
    label_Fa = "Fa" + label_suffix
    Fn,f,a = pfp_utils.GetSeriesasMA(ds, label_Fn)
    Fg,f,a = pfp_utils.GetSeriesasMA(ds, label_Fg)
    Fa = Fn - Fg
    attr = pfp_utils.MakeAttributeDictionary(long_name = ('Calculated '
                                                          'available '
                                                          'energy'),
                                             standard_name = 'not defined',
                                             units='W/m2')
    pfp_utils.CreateSeries(ds, label_Fa, Fa, Flag = f, Attr = attr)
    return
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_groundheatflux(ds, label_suffix):

    label_Fg = "Fg" + label_suffix
    label_Fn = "Fn" + label_suffix
    label_Fh = "Fh" + label_suffix
    label_Fe = "Fe" + label_suffix
    Fn,f,a = pfp_utils.GetSeriesasMA(ds, label_Fn)
    Fh,f,a = pfp_utils.GetSeriesasMA(ds, label_Fh)
    Fe,f,a = pfp_utils.GetSeriesasMA(ds, label_Fe)
    Fg = Fn - Fh - Fe
    attr = pfp_utils.MakeAttributeDictionary(long_name = ('Calculated ground '
                                                          'heat flux'),
                                             standard_name = ('downward_heat_'
                                                              'flux_in_soil'),
                                             units='W/m2')
    pfp_utils.CreateSeries(ds, label_Fg, Fg, Flag = f, Attr = attr)
    return
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_radiation(ds, label_suffix):

    label_Fn = "Fn" + label_suffix
    label_Fsd = "Fsd" + label_suffix
    label_Fld = "Fld" + label_suffix
    label_Fsu = "Fsu"  + label_suffix
    label_Flu = "Flu" + label_suffix
    label_Fn_sw = "Fn_sw" + label_suffix
    label_Fn_lw = "Fn_lw" + label_suffix
    Fsd,f,a = pfp_utils.GetSeriesasMA(ds, label_Fsd)
    Fld,f,a = pfp_utils.GetSeriesasMA(ds, label_Fld)
    Fn_sw,f,a = pfp_utils.GetSeriesasMA(ds, label_Fn_sw)
    Fn_lw,f,a = pfp_utils.GetSeriesasMA(ds, label_Fn_lw)
    Fsu = Fsd - Fn_sw
    Flu = Fld - Fn_lw
    Fn = (Fsd - Fsu) + (Fld - Flu)
    attr = pfp_utils.MakeAttributeDictionary(long_name = 'Up-welling long wave',
                                             standard_name = ('surface_upwelling' 
                                                              '_longwave_flux'
                                                              '_in_air'),
                                                              units = 'W/m2')
    pfp_utils.CreateSeries(ds, label_Flu, Flu, Flag = f, Attr = attr)
    attr = pfp_utils.MakeAttributeDictionary(long_name = 'Up-welling short wave',
                                             standard_name = ('surface_upwelling'
                                                              '_shortwave_flux_'
                                                              'in_air'),
                                                              units = 'W/m2')
    pfp_utils.CreateSeries(ds, label_Fsu, Fsu, Flag = f, Attr = attr)
    attr = pfp_utils.MakeAttributeDictionary(long_name='Calculated net radiation',
                                             standard_name = ('surface_net_'
                                                              'allwave_radiation'),
                                                              units = 'W/m2')
    pfp_utils.CreateSeries(ds, label_Fn, Fn, Flag = f, Attr = attr)
    return
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_regular_cuml_precip(ds, label_suffix):
    
    label = "Precip" + label_suffix
    rain_access, flag, attr = pfp_utils.GetSeries(ds, label)
    rain_inst = numpy.ediff1d(rain_access, to_begin = 0)
    hr_utc = map(lambda x: x.hour, ds.series['DateTime_UTC']['Data'])
    idx_obs = numpy.mod(hr_utc, 6) == 0
    rain_inst_mod = numpy.ma.where(idx_obs, rain_access, rain_inst)
    idx_flaginvalid = numpy.where(flag!=0)[0]
    rain_inst_mod[idx_flaginvalid] = float(c.missing_value)
    idx_imprecision = numpy.ma.where(rain_inst_mod < 0.01)[0]
    rain_inst_mod[idx_imprecision] = float(0)
    rain_accum = numpy.ma.cumsum(rain_inst_mod)
    attr["long_name"] = "Precipitation total over time step"
    attr["units"] = "mm/60 minutes"
    pfp_utils.CreateSeries(ds, label, rain_accum, Flag = flag, Attr = attr)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
#def get_regular_precip(ds, label_suffix):
#    
#    label = "Precip" + label_suffix
#    rain_access, flag, attr = pfp_utils.GetSeries(ds, label)
#    rain_inst = numpy.ediff1d(rain_access, to_begin = 0)
#    hr_utc = map(lambda x: x.hour, ds.series['DateTime_UTC']['Data'])
#    idx_obs = numpy.mod(hr_utc, 6) == 0
#    rain_inst_mod = numpy.ma.where(idx_obs, rain_access, rain_inst)
#    idx_flaginvalid = numpy.where(flag!=0)[0]
#    rain_inst_mod[idx_flaginvalid] = float(c.missing_value)
#    idx_imprecision = numpy.ma.where(rain_inst_mod < 0.01)[0]
#    rain_inst_mod[idx_imprecision] = float(0)
#    rain_accum = numpy.ma.cumsum(rain_inst_mod)
#    attr["long_name"] = "Precipitation total over time step"
#    attr["units"] = "mm/60 minutes"
#    pfp_utils.CreateSeries(ds, label, rain_accum, Flag = flag, Attr = attr)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_relativehumidity(ds, label_suffix):

    q_label = "q" + label_suffix
    Ta_label = "Ta" + label_suffix
    ps_label = "ps" + label_suffix
    RH_label = "RH" + label_suffix
    q,f,a = pfp_utils.GetSeriesasMA(ds, q_label)
    Ta,f,a = pfp_utils.GetSeriesasMA(ds, Ta_label)
    ps,f,a = pfp_utils.GetSeriesasMA(ds, ps_label)
    RH = mf.RHfromspecifichumidity(q, Ta, ps)
    attr = pfp_utils.MakeAttributeDictionary(long_name = 'Relative humidity',
                                           units = '%',
                                           standard_name = 'not defined')
    pfp_utils.CreateSeries(ds, RH_label, RH, Flag = f, Attr = attr)
    return
#------------------------------------------------------------------------------
    
#------------------------------------------------------------------------------
def get_windspeedanddirection(ds, label_suffix):

    
    u_label = "u" + label_suffix
    v_label = "v" + label_suffix  
    Ws_label = "Ws" + label_suffix
    Wd_label = "Wd" + label_suffix
    u,f,a = pfp_utils.GetSeriesasMA(ds,u_label)
    v,f,a = pfp_utils.GetSeriesasMA(ds,v_label)
    
    Ws = numpy.sqrt(u * u + v * v)
    attr = pfp_utils.MakeAttributeDictionary(long_name = "Wind speed",
                                             units = "m/s", height = "10m")
    pfp_utils.CreateSeries(ds,Ws_label,Ws,Flag=f,Attr=attr)
    
    attr = pfp_utils.MakeAttributeDictionary(long_name="Wind direction",
                                           units="degrees",height="10m")
    Wd = float(270) - numpy.ma.arctan2(v, u)*float(180)/numpy.pi
    index = numpy.ma.where(Wd>360)[0]
    if len(index) > 0: Wd[index] = Wd[index] - float(360)
    pfp_utils.CreateSeries(ds, Wd_label, Wd, Flag = f, Attr = attr)
    return    
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def interpolate_to_30minutes(ds_60minutes):
    ds_30minutes = pfp_io.DataStructure()
    # copy the global attributes to the 30 minutes ds and update time step 
    for this_attr in ds_60minutes.globalattributes.keys():
        ds_30minutes.globalattributes[this_attr] = ds_60minutes.globalattributes[this_attr]
    ds_30minutes.globalattributes["time_step"] = 30
    # generate the 30 minute datetime series
    dt_loc_30minutes = pd.date_range(ds_60minutes.series["DateTime"]["Data"][0],
                                     ds_60minutes.series["DateTime"]["Data"][-1],
                                     freq = '30T').to_pydatetime()
    dt_utc_30minutes = pd.date_range(ds_60minutes.series["DateTime_UTC"]["Data"][0],
                                     ds_60minutes.series["DateTime_UTC"]["Data"][-1],
                                     freq = '30T').to_pydatetime()
    # update the global attribute "nc_nrecs"
    ds_30minutes.globalattributes['nc_nrecs'] = len(dt_loc_30minutes)
    ds_30minutes.series["DateTime"] = {}
    ds_30minutes.series["DateTime"]["Data"] = dt_loc_30minutes
    flag = numpy.zeros(len(dt_loc_30minutes),dtype=numpy.int32)
    ds_30minutes.series["DateTime"]["Flag"] = flag
    ds_30minutes.series["DateTime_UTC"] = {}
    ds_30minutes.series["DateTime_UTC"]["Data"] = dt_utc_30minutes
    flag = numpy.zeros(len(dt_utc_30minutes),dtype=numpy.int32)
    ds_30minutes.series["DateTime_UTC"]["Flag"] = flag
    # get the year, month etc from the datetime
    pfp_utils.get_xldatefromdatetime(ds_30minutes)
    pfp_utils.get_ymdhmsfromdatetime(ds_30minutes)
    # interpolate to 30 minutes
    nRecs_60 = len(ds_60minutes.series["DateTime"]["Data"])
    nRecs_30 = len(ds_30minutes.series["DateTime"]["Data"])
    x_60minutes = numpy.arange(0,nRecs_60,1)
    x_30minutes = numpy.arange(0,nRecs_60-0.5,0.5)
    varlist_60 = ds_60minutes.series.keys()
    # strip out the date and time variables already done
    for item in ["DateTime","DateTime_UTC","xlDateTime","Year","Month","Day","Hour","Minute","Second","Hdh","Hr_UTC"]:
        if item in varlist_60: varlist_60.remove(item)
    # now do the interpolation (its OK to interpolate accumulated precipitation)
    for label in varlist_60:
        series_60minutes,flag,attr = pfp_utils.GetSeries(ds_60minutes,label)
        ci_60minutes = numpy.zeros(len(series_60minutes))
        idx = numpy.where(abs(series_60minutes-float(c.missing_value))<c.eps)[0]
        ci_60minutes[idx] = float(1)
        int_fn = interp1d(x_60minutes,series_60minutes)
        series_30minutes = int_fn(x_30minutes)
        int_fn = interp1d(x_60minutes,ci_60minutes)
        ci_30minutes = int_fn(x_30minutes)
        idx = numpy.where(abs(ci_30minutes-float(0))>c.eps)[0]
        series_30minutes[idx] = numpy.float64(c.missing_value)
        flag_30minutes = numpy.zeros(nRecs_30, dtype=numpy.int32)
        flag_30minutes[idx] = numpy.int32(1)
        pfp_utils.CreateSeries(ds_30minutes,label,series_30minutes,Flag=flag_30minutes,Attr=attr)
    # get the UTC hour
    hr_utc = [float(x.hour)+float(x.minute)/60 for x in dt_utc_30minutes]
    attr = pfp_utils.MakeAttributeDictionary(long_name='UTC hour')
    flag_30minutes = numpy.zeros(nRecs_30, dtype=numpy.int32)
    pfp_utils.CreateSeries(ds_30minutes,'Hr_UTC',hr_utc,Flag=flag_30minutes,Attr=attr)
    return ds_30minutes
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def makedummyseries(shape):
    return numpy.ma.masked_all(shape)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_ozflux_site_list(master_file_path):
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
    df['Time_zone'] = map(lambda x: tzf().timezone_at(lat = x[0], lng = x[1]), 
                          zip(df.Latitude.tolist(), df.Longitude.tolist()))
    df['input_path'] = map(lambda x: os.path.join(input_data_path, 
                           '{}.nc'.format(x)), df.index)
    df['output_path'] = map(lambda x: os.path.join(output_data_path, 
                            '{}.nc'.format(x)), df.index)
    return df

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
            'Habl': 'abl_ht'}
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def read_access_file(path):
    '''Read data from access file into temporary class'''
    
    # Initialise class
    var_dict = makevardict()
    var_dict.update({'time': 'time'})
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
            for attr in ncfile.variables[access_name].ncattrs():
                f.varattr[access_name][attr] = getattr(ncfile.variables
                                                       [access_name], attr)
        except AssertionError:
            f.variables[access_name] = makedummyseries(shape)

    ncfile.close()

    return f
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def set_globalattributes(ds_60minutes, info):
    ds_60minutes.globalattributes["latitude"] = info.Latitude
    ds_60minutes.globalattributes["longitude"] = info.Longitude
    ds_60minutes.globalattributes["nc_level"] = "L1"
    ds_60minutes.globalattributes["site_name"] = info.Site
    ds_60minutes.globalattributes["time_step"] = 60
    ds_60minutes.globalattributes["time_zone"] = info.Time_zone
    ds_60minutes.globalattributes["xl_datemode"] = 0
    ds_60minutes.globalattributes['nc_nrecs'] = len(ds_60minutes.series
                                                    ["DateTime"]["Data"])
    return
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def write_to_ds(f, info):
    '''Transfer data to PFP data structure'''
    
    ds_60minutes = pfp_io.DataStructure()
    
    # list of variables to process
    var_dict = makevardict()
    var_list = var_dict.keys()

    # get the python datetime and convert from utc; also create an index of 
    # valid dates that is used to select valid cases from other variables
    base_date_str = ' '.join(f.varattr['time']['units'].split(' ')[2:4])
    base_date = dt.datetime.strptime(base_date_str, '%Y-%m-%d %H:%M:%S')
    hours = (f.variables['time'] * 24).astype(int)
    if f.variables['time'].mask == False: 
        idx = numpy.arange(len(f.variables['time']))
    else:
        idx = numpy.where(~f.variables['time'].mask)[0]
    utc_dates = map(lambda x: (base_date + dt.timedelta(hours = x))
                               .replace(tzinfo=pytz.utc), hours[idx])
    tz = timezone(info.Time_zone)
    local_dates = map(lambda x: x.astimezone(tz), utc_dates)
    local_dates = map(lambda x: (x - x.dst()).replace(tzinfo=None), local_dates)
    
    # Put local and utc datetimes into the data structure
    flag = numpy.zeros(len(local_dates),dtype=numpy.int32)
    ds_60minutes.series["DateTime"] = {}
    ds_60minutes.series["DateTime"]["Data"] = local_dates
    ds_60minutes.series["DateTime"]["Flag"] = flag
    ds_60minutes.series["DateTime_UTC"] = {}
    ds_60minutes.series["DateTime_UTC"]["Data"] = utc_dates
    ds_60minutes.series["DateTime_UTC"]["Flag"] = flag

    # Do stuff - why do we need the hours?
    flag_60minutes = numpy.zeros(len(ds_60minutes.series["DateTime"]["Data"]), 
                                 dtype = numpy.int32)
    attr = pfp_utils.MakeAttributeDictionary(long_name = 'UTC hour')
    pfp_utils.CreateSeries(ds_60minutes,'Hr_UTC', hours, Flag = flag_60minutes,
                           Attr = attr)

    # now loop over the variables listed in the control file
    for label in var_list:
        
        # get the name of the variable in the ACCESS file
        access_name = var_dict[label]
        if access_name not in f.variables.keys():
            msg = "Requested variable "+access_name
            msg = msg+" not found in ACCESS data"
#            logging.error(msg)
            continue
        
        varattr = f.varattr[access_name]
        varattr["missing_value"] = c.missing_value

        # loop over the 3x3 matrix of ACCESS grid data supplied
        for i in range(0, 3):
            for j in range(0, 3):
                label_ij = label + '_' + str(i) + str(j)
                if len(f.variables[access_name].shape) == 3:
                    series = f.variables[access_name][:, i, j]
                elif len(f.variables[access_name].shape) == 4:
                    series = f.variables[access_name][:, 0, i, j]
                else:
                    msg = "Unrecognised variable ("+label
                    msg = msg+") dimension in ACCESS file"
#                    logging.error(msg)
                series = series[idx]
                pfp_utils.CreateSeries(ds_60minutes, label_ij, series,
                                       Flag = flag_60minutes, Attr = varattr)
    
    return ds_60minutes
#------------------------------------------------------------------------------

master_file_path = '/home/ian/Temp/site_master.xls'
input_data_path = '/home/ian/Temp/access_nc/201901'
output_data_path = '/home/ian/Temp/pyflux_nc/201901'

sites_df = make_ozflux_site_list(master_file_path)

for site in sites_df.index[:1]:
    
    info_df = sites_df.loc[site]
    access_data = read_access_file(info_df.input_path)
    ds_60minute = write_to_ds(access_data, info_df)
    set_globalattributes(ds_60minute, info_df)
#    logging.info("Checking for time gaps")
    if pfp_utils.CheckTimeStep(ds_60minute):
        pfp_utils.FixTimeStep(ds_60minute)
    # get the datetime in some different formats
#    logging.info('Getting xlDateTime and YMDHMS')
    pfp_utils.get_xldatefromdatetime(ds_60minute)
    pfp_utils.get_ymdhmsfromdatetime(ds_60minute)
#    logging.info("Changing units and getting derived quantities")
    do_unit_conversions(ds_60minute) # Change units and calculate quantities
    if info_df.Time_step == 30: # Interpolate to 30 minutes where appropriate
        ds_30minute = interpolate_to_30minutes(ds_60minute)    