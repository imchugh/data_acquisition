#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 14 12:54:26 2017

@author: ian
"""

import os
import pdb
import xlrd

# get the site information and the AWS stations to use
def get_bom_site_details(path_to_file, sheet_name, var_to_get):

    valid_vars = ['BoM ID', 'Name', 'Elevation', 'latitude', 'Longitude']
    if not var_to_get in valid_vars:
        raise KeyError('{0} is not a valid identifier; the following '
                       'variables can be fetched: {1}'
                       .format(var_to_get), ', '.join(valid_vars))
    
    wb = xlrd.open_workbook(path_to_file)
    sheet = wb.sheet_by_name(sheet_name)
    header_row = sheet.row_values(9)
    
    target_key = 'Name' if var_to_get == 'BoM ID' else 'BoM ID'
    key_idxs = [i for i, var in enumerate(header_row) if target_key in  var]
    var_idxs = [i for i, var in enumerate(header_row) if var_to_get in var]
    if var_to_get in valid_vars[2:]: var_idxs = var_idxs[1:]
    idxs = zip(key_idxs, var_idxs)
    
    xl_row = 10
    id_list = []
    var_list = []
    for row in range(xl_row,sheet.nrows):
        xlrow = sheet.row_values(row)
        for i in idxs:
            key = xlrow[i[0]]
            var = xlrow[i[1]]
            try:
                if target_key == 'BoM ID':
                    key = str(int(key)).zfill(6)
                elif target_key == 'Name':
                    var = str(int(var)).zfill(6)
                id_list.append(key)
                var_list.append(var)
            except ValueError:
                continue
                
    return dict(zip(id_list, var_list))

def convert_kmhr_2_ms(km_hr):
    
    try:
        return str(round(float(km_hr)/3.6, 1)).rjust(5)
    except:
        return km_hr.rjust(5)

def get_trueNorth_from_compass(compass_str):
    
    compass_dict = {'N': '0',
                    'NNE': '22',
                    'NE': '45',
                    'ENE': '68',
                    'E': '90',
                    'ESE': '112',
                    'SE': '135',
                    'SSE': '157',
                    'S': '180',
                    'SSW': '203',
                    'SW': '225',
                    'WSW': '247',
                    'W': '270',
                    'WNW': '293',
                    'NW': '315',
                    'NNW': '337'}
    
    
    try:
        return compass_dict[compass_str.strip()].rjust(3)
    except:
        return '  0'

def slp_2_stnlp(p0, site_alt):

    try:
        p0_pa = float(p0) * 100
    except ValueError:
        return p0
    
    L = 0.0065
    R = 8.3143
    T0 = 288.15
    g = 9.80665
    M = 0.0289644
    
    A = (g * M) / (R * L)
    B = L / T0
    
    p = (p0_pa * (1 - B * site_alt) ** A) / 100

    return str(round(p, 1)).zfill(6)

def get_stnlp_from_slp(slp, altitude):
    
    return '1013.0'

in_path = '/home/ian/BOM_data/'
out_path = '/home/ian/BOM_data/converted'
#f_list = ['bom_station_009053.txt']
xlname = '/home/ian/Temp/AWS_Locations.xls'
    
target_header_list = ['hm',
                      'Station Number',
                      'Year Month Day Hour Minutes in YYYY',
                      'MM',
                      'DD',
                      'HH24',
                      'MI format in Local time',
                      'Year Month Day Hour Minutes in YYYY',
                      'MM',
                      'DD',
                      'HH24',
                      'MI format in Local standard time',
                      'Precipitation since 9am local time in mm',
                      'Quality of precipitation since 9am local time',
                      'Air Temperature in degrees C',
                      'Quality of air temperature',
                      'Dew point temperature in degrees C',
                      'Quality of dew point temperature',
                      'Relative humidity in percentage %',
                      'Quality of relative humidity',
                      'Wind speed in m/s',
                      'Wind speed quality',
                      'Wind direction in degrees true',
                      'Wind direction quality',
                      'Speed of maximum windgust in last 10 minutes in m/s',
                      'Quality of speed of maximum windgust in last 10 minutes',
                      'Station level pressure in hPa',
                      'Quality of station level pressure',
                      'AWS Flag',
                      '#\r\n']

func_dict = {'Speed of maximum windgust in '
             'last 10 minutes in m/s': convert_kmhr_2_ms,
             'Wind direction in degrees true': get_trueNorth_from_compass,
             'Wind speed in m/s': convert_kmhr_2_ms,
             'Station level pressure in hPa': slp_2_stnlp}

swap_dict = {'Quality of station level pressure': 
                 'Quality of mean sea level pressure',
             'MI format in Local time': 'MI format in Local standard time',
             'Station level pressure in hPa': 'Mean sea level pressure in hPa',
             'Speed of maximum windgust in last 10 minutes in m/s':
                 'Speed of maximum windgust in last 10 minutes in  km/h',
             'Wind direction in degrees true': 
                 'Wind direction in 16 compass points',
            'Wind speed in m/s': 'Wind speed in km/h'}

elevation_dict = get_bom_site_details(xlname, 'OzFlux', 
                                      var_to_get = 'Elevation')

files = (f for f in os.listdir(in_path) 
         if os.path.isfile(os.path.join(in_path, f)))
rslt_dict = {}
for fname in files:
    
    try:
        site_id = os.path.splitext(fname)[0].split('_')[2]
    except:
        pdb.set_trace()
    elevation = elevation_dict[site_id]
    
    in_fpname = os.path.join(in_path, fname)
    out_fpname = os.path.join(out_path, fname)
    
    with open(in_fpname) as in_f, open(out_fpname, 'w') as out_f:
        
        out_f.write(','.join(target_header_list))

        header = in_f.readline()
        header_list = header.split(',')
        header_dict = {var: i for i, var in enumerate(header_list)}
        for in_line in in_f:
            in_list = in_line.split(',')
            out_list = []
            for var in target_header_list:
                try:
                    index = header_dict[var]
                    value = in_list[index]
                    out_list.append(value)
                except KeyError:
                    alias = swap_dict[var]
                    index = header_dict[alias]
                    value = in_list[index]
                    if not var in func_dict:
                        out_list.append(value)
                    else:
                        if var == 'Station level pressure in hPa':
                            value = func_dict[var](value, elevation)
                        else:
                            value = func_dict[var](value)
                        out_list.append(value)
            out_line = ','.join(out_list)
            out_f.write(out_line)
                