#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Thu Jul  5 16:32:38 2018

@author: ian
"""

from bs4 import BeautifulSoup
import datetime as dt
import netCDF4
import numpy as np
import numpy.ma as ma
import os
import pandas as pd
from pytz import timezone
import requests
from timezonefinder import TimezoneFinder as tzf
import xlrd
      
#------------------------------------------------------------------------------
# PROCESSING ALGORITHMS
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def check_seen_records(sites_df, data_file_path, server_ID_list):
    """Cross-check existing file data against available data on server"""

    d = {'date_time': lambda x: dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')}
    bool_df = pd.DataFrame(index = server_ID_list)
    for site in sites_df.index:
        name = '_'.join(site.split(' '))
        f_name = '{}_ACCESS.csv'.format(name)
        if not f_name in os.listdir(data_file_path): 
            bool_df[name] = np.tile(False, len(server_ID_list))
            continue
        df = pd.read_csv(os.path.join(data_file_path, f_name), 
                         usecols = ['date_time'], converters = d,
                         index_col = 'date_time')
        local_dates = pd.date_range(df.index[0], df.index[-1], freq = '60T')
        utc_dates = convert_utc(local_dates, sites_df.loc[site, 'Latitude'], 
                                sites_df.loc[site, 'Longitude'], 'to_utc')
        local_ID_list = []
        for date in utc_dates:
            mod_hour = np.mod(date.hour, 6)
            str_mod = str(mod_hour).zfill(3)
            new_date = date - dt.timedelta(hours = mod_hour)
            local_ID_list.append('{0}_{1}'.format(
                                   dt.datetime.strftime(new_date, '%Y%m%d%H'), 
                                   str_mod))
        bool_df[name] = map(lambda x: x in local_ID_list, server_ID_list)
    d = {}
    bool_df = bool_df.T
    for col in bool_df.columns:
        l = list(bool_df[bool_df[col]==False].index)
        d[col] = l
    return d
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def convert_utc(dates, lat, lon, direction):
    """Convert to or from utc"""
    
    if not direction in ['from_utc', 'to_utc']:
        raise KeyError('direction parameter must be either to_utc or from_utc')
    tz = timezone(tzf().timezone_at(lat = lat, lng = lon))
    if direction == 'to_utc':
        return map(lambda x: x - (tz.utcoffset(x) - tz.dst(x)), dates)
    else:
        return map(lambda x: x + (tz.utcoffset(x) - tz.dst(x)), dates)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_fileID_from_date(dates, ):
    """Convert dates into the directory labelling format on opendap server"""

    if not isinstance(dates, list): dates = [dates]    
    fmt_list = []
    for date in dates:
        for i in range(6):
            num_str = str(i).zfill(3)
            fmt_list.append('{0}_{1}'.format(date, num_str))
    return fmt_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_filepath_from_ID(ID):

    date = ID.split('_')[0]
    fname = 'ACCESS-R_{}_surface.nc'.format(ID)
    return os.path.join(opendap_base_url, date, fname)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_ozflux_site_list(master_file_path):
    '''Create a dataframe containing site names (index) and lat, long and 
       measurement interval'''
    
    wb = xlrd.open_workbook(master_file_path)
    sheet = wb.sheet_by_name('Active')
    header_row = 9
    header_list = sheet.row_values(header_row)
    df = pd.DataFrame()
    for var in ['Site', 'Latitude', 'Longitude', 'Time step']:
        index_val = header_list.index(var)
        df[var] = sheet.col_values(index_val, header_row + 1)   
    df.index = map(lambda x: '_'.join(x.split(' ')), df.Site)
    df.drop(header_list[0], axis = 1, inplace = True)
    return df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_site_coordinate_indices(coords_s, lats_list, lons_list):
    
    delta = 0.165
    lat_indices = [i for i, x in enumerate(lats_list) 
                   if (coords_s['Latitude'] - delta) <= x <= 
                      (coords_s['Latitude'] + delta)]
    lon_indices = [i for i, x in enumerate(lons_list) 
                   if (coords_s['Longitude'] - delta) <= x <= 
                      (coords_s['Longitude'] + delta)]
    return {'lat': lat_indices, 'lon': lon_indices}
#------------------------------------------------------------------------------
    
#------------------------------------------------------------------------------
def get_site_data(nc, coords_s):
    '''Get date and time, then iterate through variable list and call 
       subroutine to get site data subset from nc file'''
    
    indices_dict = get_site_coordinate_indices(coords_s,
                                               nc.variables['lat'][:].data,
                                               nc.variables['lon'][:].data)

    base_date_str = getattr(nc.variables['time'], 'units')
    base_date = dt.datetime.strptime(' '.join(base_date_str.split(' ')[2:4]), 
                                     '%Y-%m-%d %H:%M:%S')
    hour = nc.variables['time'][:].data.item() * 24
    base_date_time = base_date + dt.timedelta(hours = hour)
    temp_dict = {'date_time_utc': base_date_time}
    local_time = convert_utc([base_date_time], coords_s['Latitude'], 
                             coords_s['Longitude'], 'from_utc')[0]
    temp_dict['date_time_local'] = local_time
    
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
def get_subset_from_nc(nc, indices_dict, var_name, dim_2 = None):
    '''Retrieve data subset for site location and do basic QC'''
    
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
        try: 
            if not combined_ma[1, 1].mask: return combined_ma[1, 1].data
        except AttributeError: 
            return np.nan
        return np.nan
    return unmasked_arr.mean()
#------------------------------------------------------------------------------    

#------------------------------------------------------------------------------
def list_opendap_dirs(url, ext = 'html'):
    """Scrape list of directories from opendap surface url"""
    
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')    
    dir_list = [url + '/' + node.get('href') for node in soup.find_all('a') 
                if node.get('href').endswith(ext)]
    new_list = []
    for path in dir_list:
        path_list = path.replace('//', '/').split('/')[1:]
        try:
            path_list.remove('catalog.html')
            dt.datetime.strptime(path_list[-1], '%Y%m%d%H')
            new_list.append(path_list[-1])
        except: 
            continue
    return new_list
#------------------------------------------------------------------------------
    
#------------------------------------------------------------------------------
def make_new_df(data_list, site, interval):
    '''Run all conversions and calculations and produce new dataframe'''
    
    d = {'date_time': lambda x: dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')}
    interval_str = '{}T'.format(str(int(interval)))
    df = pd.DataFrame(data_list)
    df.index = df.date_time_local
    new_df = df[['Habl', 'Fsd', 'Fld', 'Fh', 'Fe']].copy()
    for var in filter(lambda x: 'Ts' in x, df.columns):
        new_df[var] = convert_Kelvin_to_celsius(df[var])
    new_df['Precip'] = convert_rainfall(df)
    new_df['Ta'] = convert_Kelvin_to_celsius(df.Ta)
    new_df['Rh'] = get_Rh(df)
    new_df['Ah'] = get_Ah(df)
    new_df = new_df.join(get_energy_components(df))
    target = os.path.join(output_dir, '{}_ACCESS.csv'.format(site))
    if os.path.exists(target):
        ex_df = pd.read_csv(target, converters = d, index_col = 'date_time')
        new_df = pd.concat([ex_df, new_df])
    new_df.sort_index(inplace = True)
    new_df = new_df.resample(interval_str).interpolate()
    new_df.to_csv(target, index_label = 'date_time')
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
    new_s = pd.Series(np.where(idx, s, diff_s), index = df.index)
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

opendap_base_url = ('http://opendap.bom.gov.au:8080/thredds/dodsC/bmrc/'
                    'access-r-fc/ops/surface/')
output_dir = '/home/ian/Desktop/ACCESS'

sites_df = get_ozflux_site_list('/home/ian/Temp/site_master.xls')
dir_list = list_opendap_dirs(opendap_base_url)
master_ID_list = get_fileID_from_date(dir_list)
seen_files_dict = check_seen_records(sites_df, output_dir, master_ID_list)

results_dict = {}
for this_dir in dir_list:
    print 'Parsing date {}:'.format(this_dir)
    ID_list = get_fileID_from_date(this_dir)
    for ID in ID_list:
        file_path = get_filepath_from_ID(ID)
        file_name = file_path.split('/')[-1]
        print '- File {}...'.format(file_name),
        sites_list = seen_files_dict[ID]
        if not sites_list:
            print 'previously seen and written! Skipping'
            continue
        nc = netCDF4.Dataset(file_path)
        print 'retrieving data for site:'
        for site in sites_list:
            print '    {}'.format(site)
            if not site in results_dict.keys(): 
                results_dict[site] = []
            raw_dict = get_site_data(nc, sites_df.loc[site])
            results_dict[site].append(raw_dict)
      
for site in sorted(results_dict.keys()):
    df = make_new_df(results_dict[site], site, sites_df.loc[site, 'Time step'])