#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jul  5 16:32:38 2018

@author: ian
"""

import datetime as dt
import netCDF4
import numpy as np
import numpy.ma as ma
import os
import pandas as pd
import pdb
from pytz import timezone
from timezonefinder import TimezoneFinder as tzf
import xlrd
      
#------------------------------------------------------------------------------
# PROCESSING ALGORITHMS
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_subset_from_nc(nc, indices_dict, var_name, dim_2 = None):

    lat_idx, lon_idx = indices_dict['lat'], indices_dict['lon']
    if dim_2 is None:
        sub_arr = (nc.variables[var_name][0][:]
                   [lat_idx[0]: lat_idx[-1] + 1, lon_idx[0]: lon_idx[-1] + 1])
    else:
        sub_arr = (nc.variables[var_name][0][dim_2]
                   [lat_idx[0]: lat_idx[-1] + 1, lon_idx[0]: lon_idx[-1] + 1])
    this_range = range_dict[var_name]
    data = sub_arr.data
    first_mask = sub_arr.mask
    less_mask = ma.masked_less(data, this_range[0]).mask
    greater_mask = ma.masked_greater(data, this_range[1]).mask
    fill_mask = ma.masked_equal(data, sub_arr.fill_value).mask
    combined_mask = first_mask + less_mask + greater_mask + fill_mask
    combined_ma = ma.array(data, mask = combined_mask)
    unmasked_arr = combined_ma[~combined_ma.mask].data
    if not len(unmasked_arr) > len(sub_arr) / 2:
        if not combined_ma[1, 1].mask: return combined_ma[1, 1].data
        return np.nan
    return unmasked_arr.mean()
#------------------------------------------------------------------------------    
    
#------------------------------------------------------------------------------
def get_site_data(nc, coords_dict):

    indices_dict = get_site_coordinate_indices(coords_dict,
                                               nc.variables['lat'][:].data,
                                               nc.variables['lon'][:].data)

    base_date_str = getattr(nc.variables['time'], 'units')
    hour = nc.variables['time'][:].data.item() * 24
    base_date = dt.datetime.strptime(' '.join(base_date_str.split(' ')[2:4]), 
                                     '%Y-%m-%d %H:%M:%S')
    base_date_time = base_date + dt.timedelta(hours = hour)
    time_zone = timezone(tzf().timezone_at(lat = coords_dict['lat'], 
                                           lng = coords_dict['lon']))
    utc_offset = time_zone.utcoffset(base_date) - time_zone.dst(base_date)
    temp_dict = {'date_time_utc': base_date_time}
    temp_dict = {'date_time': base_date_time + utc_offset}
    
    soil_vars = ['Sws', 'Ts']
    soil_lvl_ma = nc.variables['soil_lvl'][:]
    soil_lvl = [str(x) for x in soil_lvl_ma[~soil_lvl_ma.mask]]

    for name in vars_dict.keys():
        access_name = vars_dict[name]
        if name in soil_vars:
            for i, depth in enumerate(soil_lvl):
                new_name = '{0}_{1}m'.format(name, depth)    
                temp_dict[new_name] = get_subset_from_nc(nc, indices_dict,
                                                         access_name, i)
        else:
            temp_dict[name] = get_subset_from_nc(nc, indices_dict, access_name)    

    return temp_dict
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_ozflux_site_list(master_file_path):
    
    wb = xlrd.open_workbook(master_file_path)
    sheet = wb.sheet_by_name('Active')
    header_row = 9
    header_list = sheet.row_values(header_row)
    df = pd.DataFrame()
    for var in ['Site', 'Latitude', 'Longitude']:
        index_val = header_list.index(var)
        df[var] = sheet.col_values(index_val, header_row + 1)   
    df.index = df[header_list[0]]
    df.drop(header_list[0], axis = 1, inplace = True)
    return df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_site_coordinate_indices(coords_dict, lats_list, lons_list):
    
    delta = 0.165
    lat_indices = [i for i, x in enumerate(lats_list) 
                   if (coords_dict['lat'] - delta) <= x <= 
                      (coords_dict['lat'] + delta)]
    lon_indices = [i for i, x in enumerate(lons_list) 
                   if (coords_dict['lon'] - delta) <= x <= 
                      (coords_dict['lon'] + delta)]
    return {'lat': lat_indices, 'lon': lon_indices}
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def make_new_df(data_list):
    
    df = pd.DataFrame(data_list)
    df['date_time_utc'] = df.date_time - dt.timedelta(hours=10)
    new_df = df[['date_time', 'date_time_utc', 'Habl', 'Fsd', 'Fld', 
                'Fh', 'Fe']].copy()
    for var in filter(lambda x: 'Ts' in x, df.columns):
        new_df[var] = convert_Kelvin_to_celsius(df[var])
    new_df['Precip'] = convert_rainfall(df)
    new_df['Ta'] = convert_Kelvin_to_celsius(df.Ta)
    new_df['Rh'] = get_Rh(df)
    new_df['Ah'] = get_Ah(df)
    new_df = new_df.join(get_energy_components(df))
    new_df.index = new_df.date_time
    new_df.drop(['date_time', 'date_time_utc'], axis = 1, inplace = True)
    return new_df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def write_to_file(df, site_name):
    
    joined_site_name = '_'.join(site_name.split(' '))
    target = os.path.join(output_dir, '{}_ACCESS.csv'.format(joined_site_name))
    if os.path.exists(target):
        ex_df = pd.read_csv(target)
        full_df = pd.concat([ex_df, df])
        full_df.drop_duplicates(inplace = True)
        full_df.to_csv(target, index_label = 'date_time')
    else:
        df.to_csv(target, index_label = 'date_time')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# CONVERSION ALGORITHMS
#------------------------------------------------------------------------------

def convert_Kelvin_to_celsius(s):
    
    return s - 273.15
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def convert_pressure(df):
    
    return df.ps / 1000.0    
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def convert_rainfall(df):
    
    s = df.Precip
    diff_s = s - s.shift()
    idx = np.mod(map(lambda x: x.hour, df.date_time_utc), 6)==0
    new_s = pd.Series(np.where(idx, s, diff_s))
    new_s.loc[new_s < 0.01] = 0
    return new_s
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_Ah(df):
    
    return get_e(df) * 10**6 / (df.Ta * 8.3143) / 18
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_e(df):

    Md = 0.02897   # molecular weight of dry air, kg/mol
    Mv = 0.01802   # molecular weight of water vapour, kg/mol    
    return df.q * (Md / Mv) * (df.ps / 1000)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_energy_components(df):
    
    new_df = pd.DataFrame(index = df.index)
    new_df['Fsu'] = df.Fsd - df.Fn_sw
    new_df['Flu'] = df.Fld - df.Fn_lw
    new_df['Fn'] = (df.Fsd - new_df.Fsu) + (df.Fld - new_df.Flu)
    new_df['Fa'] = df.Fh + df.Fe
    new_df['Fg'] = new_df.Fn - new_df.Fa
    return new_df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_es(df):
    
    return 0.6106 * np.exp(17.27 * (df.Ta - 273.15) / ((df.Ta - 273.15)  + 237.3))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_Rh(df):
    
    return get_e(df) / get_es(df) * 100
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_wind_direction(df):    
    
    s = float(270) - np.arctan2(df.v, df.u) * float(180) / np.pi
    s.loc[s > 360] -= float(360)
    return
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_wind_speed(df):
    
    return np.sqrt(df.u**2 + df.v**2)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# MAIN PROGRAM
#------------------------------------------------------------------------------

vars_dict = {'Fsd': 'av_swsfcdown',
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

range_dict = {'av_swsfcdown': [0, 1400],
              'av_netswsfc': [0, 1400],
              'av_lwsfcdown': [200, 600],
              'av_netlwsfc': [-300, 300],
              'temp_scrn': [230, 330],
              'qsair_scrn': [0, 1],
              'soil_mois': [0, 100],
              'soil_temp': [210, 350],
              'u10': [-50, 50],
              'v10': [-50, 50],
              'sfc_pres': [75000, 110000],
              'accum_prcp': [0, 100],
              'sens_hflx': [-200, 1000],
              'lat_hflx': [-200, 1000],
              'abl_ht': [0, 5000]}

#opendap_file_path = 'http://opendap.bom.gov.au:8080/thredds/dodsC/bmrc/access-r-fc/ops/surface/2018070506/ACCESS-R_2018070506_000_surface.nc'
opendap_base_path = ('http://opendap.bom.gov.au:8080/thredds/dodsC/bmrc/'
                     'access-r-fc/ops/surface/')
output_dir = '/home/ian/Desktop/ACCESS'

yest = dt.date.today() - dt.timedelta(1)
ymd = yest.strftime('%Y%m%d')
new_sites_df = get_ozflux_site_list('/home/ian/Temp/site_master.xls')

results_dict = {}
for this_dir in [ymd + x for x in ['00', '06', '12', '18']]:
    dir_path = os.path.join(opendap_base_path, this_dir)
    for sub_hr in [str(x).zfill(3) for x in range(6)]:
        fname = 'ACCESS-R_{0}_{1}_surface.nc'.format(this_dir, sub_hr) 
        file_path = os.path.join(dir_path, fname)
        print file_path
        nc = netCDF4.Dataset(file_path)
        for site in new_sites_df.index:
            if not site in results_dict.keys(): results_dict[site] = []
            coords_dict = {'lat': new_sites_df.loc[site, 'Latitude'],
                           'lon': new_sites_df.loc[site, 'Longitude']}
            results_dict[site].append(get_site_data(nc, coords_dict))
            
for site in sorted(results_dict.keys()):
    df = make_new_df(results_dict[site])
    name = '{}_ACCESS.csv'.format(site)
    target = os.path.join(output_dir, name)
    df.to_csv(target, index_label = 'date_time')    