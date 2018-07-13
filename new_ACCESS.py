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

class access_data(object):
    
    pass
    
    
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

range_dict = {'av_swsfcdown': [0, 1500],
              'av_netswsfc': [0, 1500],
              'av_lwsfcdown': [200, 600],
              'av_netlwsfc': [200, 600],
              'temp_scrn': [230, 330],
              'qsair_scrn': [0, 1],
              'soil_mois': [0, 1],
              'soil_temp': [210, 350],
              'u10': [0, 50],
              'v10': [0, 50],
              'sfc_pres': [75000, 110000],
              'accum_prcp': [0, 100],
              'sens_hflx': [0, 1000],
              'lat_hflx': [0, 1000],
              'abl_ht': [0, 5000]}

gmt_zone = 10

#------------------------------------------------------------------------------
def get_subset_from_nc(nc, indices_dict, var_name, dim_2 = None):

    lat_idx, lon_idx = indices_dict['lat'], indices_dict['lon']
    if not dim_2:
        sub_arr = (nc.variables[var_name][0][:]
                   [lat_idx[0]: lat_idx[1] + 1, lon_idx[0]: lon_idx[1] + 1])
    else:
        sub_arr = (nc.variables[var_name][0][dim_2]
                   [lat_idx[0]: lat_idx[1] + 1, lon_idx[0]: lon_idx[1] + 1])
    this_range = range_dict[var_name]
    data = sub_arr.data
    first_mask = sub_arr.mask
    less_mask = ma.masked_less(data, this_range[0]).mask
    greater_mask = ma.masked_greater(data, this_range[1]).mask
    fill_mask = ma.masked_equal(data, sub_arr.fill_value).mask
    combined_mask = first_mask + less_mask + greater_mask + fill_mask
    combined_ma = ma.array(data, mask = combined_mask)
    unmasked_arr = combined_ma[~combined_ma.mask].data
    if not len(unmasked_arr) > len(sub_arr) / 2: return np.nan
    return unmasked_arr.mean()
#------------------------------------------------------------------------------    
    
#------------------------------------------------------------------------------
def get_site_data(nc, coords_dict):

    indices_dict = get_site_coordinate_indices(coords_dict,
                                               nc.variables['lat'][:].data,
                                               nc.variables['lon'][:].data)

    base_date_str = getattr(nc.variables['time'], 'units')
    base_date = dt.datetime.strptime(' '.join(base_date_str.split(' ')[2:4]), 
                                     '%Y-%m-%d %H:%M:%S')
    hour = nc.variables['time'][:].data.item() * 24

    soil_vars = ['Sws', 'Ts']
    soil_lvl_ma = nc.variables['soil_lvl'][:]
    soil_lvl = [str(x) for x in soil_lvl_ma[~soil_lvl_ma.mask]]

    temp_dict = {'date_time': base_date + dt.timedelta(hours = hour + gmt_zone)}

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
def get_site_list_from_file(target):
    
    d = {}
    with open(site_file_path) as sites:
        for line in sites:
            line_list = line.split(',')
            d[line_list[0]] = {'lat': float(line_list[1]), 
                               'lon': float(line_list[2])}
    return d
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
    return {'lat': [min(lat_indices), max(lat_indices)], 
            'lon': [min(lon_indices), max(lon_indices)]}
#------------------------------------------------------------------------------

site_file_path = '/home/ian/Temp/sites_list.txt'
opendap_file_path = 'http://opendap.bom.gov.au:8080/thredds/dodsC/bmrc/access-r-fc/ops/surface/2018070506/ACCESS-R_2018070506_000_surface.nc'
opendap_base_path = ('http://opendap.bom.gov.au:8080/thredds/dodsC/bmrc/'
                     'access-r-fc/ops/surface/')
output_dir = '/home/ian/'

yest = dt.date.today() - dt.timedelta(1)
ymd = yest.strftime('%Y%m%d')
sites_dict = get_site_list_from_file(site_file_path)

results_dict = {}
for this_dir in [ymd + x for x in ['00', '06', '12', '18']]:
    dir_path = os.path.join(opendap_base_path, this_dir)
    for sub_hr in [str(x).zfill(3) for x in range(7)]:
        fname = 'ACCESS-R_{0}_{1}_surface.nc'.format(this_dir, sub_hr) 
        file_path = os.path.join(dir_path, fname)
        print file_path
        nc = netCDF4.Dataset(file_path)
        for site in ['Warra']:#sorted(sites_dict.keys()):
            if not site in results_dict.keys(): results_dict[site] = []
            results_dict[site].append(get_site_data(nc, sites_dict[site]))
#
#for site in sorted(sites_dict.keys()):
#    df = pd.DataFrame(results_dict[site])
#    df.index = df.date_time
#    df.drop('date_time', axis = 1, inplace = True)
#    results_dict[site] = df