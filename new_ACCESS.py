#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jul  5 16:32:38 2018

@author: ian
"""

import datetime as dt
import netCDF4
import numpy as np
import os
import pandas as pd
import pdb

class access_data(object):
    
    pass
    
    
#ref_dict = {'Fsd': 'av_swsfcdown',
#            'Fn_sw': 'av_netswsfc',
#            'Fld': 'av_lwsfcdown',
#            'Fn_lw': 'av_netlwsfc',
#            'Ta': 'temp_scrn',
#            'q': 'qsair_scrn',
#            'Sws': 'soil_mois',
#            'Ts': 'soil_temp',
#            'u': 'u10',
#            'v': 'v10',
#            'ps': 'sfc_pres',
#            'Precip': 'accum_prcp',
#            'Fh': 'sens_hflx',
#            'Fe': 'lat_hflx',
#            'Habl': 'abl_ht'}
#
#
#
#test_dir = '/home/ian/Temp/'
#gmt_zone = 10
#
#soil_vars = ['Sws', 'Ts']
#f_list = sorted([x for x in os.listdir(test_dir) if '.nc' in x])
#
#
#date_list = []
#data_list = []
#
#for f in f_list:
#    
#    target = os.path.join(test_dir, f)
#    nc = netCDF4.Dataset(target)
#    base_date_str = getattr(nc.variables['time'], 'units')
#    base_date = dt.datetime.strptime(' '.join(base_date_str.split(' ')[2:4]), 
#                                     '%Y-%m-%d %H:%M:%S')
#    hour = nc.variables['time'][:].data.item() * 24
#    date_list.append(base_date + dt.timedelta(hours = hour + gmt_zone))
#    soil_levels = nc.variables['soil_lvl'][:].data
#    standard_vars = filter(lambda x: not x in soil_vars, ref_dict.keys())
#    temp_dict = {}
#    for name in standard_vars:
#        access_name = ref_dict[name] 
#        if nc.variables[access_name][:].mask[0][1,1]:
#            temp_dict[name] = np.nan
#        else:
#            temp_dict[name] = nc.variables[access_name][:].data[0][1,1]
#    data_list.append(temp_dict)
#            
#test = pd.DataFrame(data_list, index = date_list)
    
#fname = 'http://opendap.bom.gov.au:8080/thredds/dodsC/bmrc/access-r-fc/ops/surface/2018070506/ACCESS-R_2018070506_000_surface.nc'
#
#nc = netCDF4(fname)

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
def get_site_coordinate_indices(coords_dict, lats_list, longs_list):
    
    delta = 0.165
    d = {}
    for site in sorted(coords_dict.keys()):
        sub_d = {}
        lat, lon = coords_dict[site]['lat'], coords_dict[site]['lon']
        lat_indices = [i for i, x in enumerate(lats_list) 
                       if (lat - delta) <= x <= (lat + delta)]
        sub_d['lat'] = [min(lat_indices), max(lat_indices)]
        long_indices = [i for i, x in enumerate(longs_list) 
                        if (lon - delta) <= x <= (lon + delta)]
        sub_d['lon'] = [min(long_indices), max(long_indices)]
        d[site] = sub_d
    return d
#------------------------------------------------------------------------------

site_file_path = '/home/ian/Temp/sites_list.txt'
opendap_file_path = 'http://opendap.bom.gov.au:8080/thredds/dodsC/bmrc/access-r-fc/ops/surface/2018070506/ACCESS-R_2018070506_000_surface.nc'

delta = 0.165

yest = dt.date.today() - dt.timedelta(1)
ymd = yest.strftime('%Y%m%d')

coords_dict = get_site_list_from_file(site_file_path)

nc = netCDF4.Dataset(opendap_file_path)

indices_dict  = get_site_coordinate_indices(coords_dict, 
                                            nc.variables['lat'][:].data, 
                                            nc.variables['lon'][:].data)

for site in sorted(indices_dict.keys())[:1]:
    
    