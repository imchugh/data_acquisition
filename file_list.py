#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 30 17:17:22 2018

@author: ian
"""

from bs4 import BeautifulSoup
import datetime as dt
import numpy as np
import os
import pandas as pd
import pdb
from pytz import timezone
import requests
from timezonefinder import TimezoneFinder as tzf
import xlrd

#------------------------------------------------------------------------------
def is_data_dir(file_str):
    """Check if is date"""
    
    sub_str = file_str.split('/')[-2]
    try:
        dt.datetime.strptime(sub_str, '%Y%m%d%H')
        return True
    except:
        return False
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def list_opendap_dirs(url, ext = 'html'):
    """Scrape list of directories from opendap surface url"""
    
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')    
    l = [url + '/' + node.get('href') for node in soup.find_all('a') 
         if node.get('href').endswith(ext)]
    return filter(lambda x: is_data_dir(x), l)
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
def get_ozflux_site_list(master_file_path):
    """Get the list of active OzFlux site names and respective coordinates"""
    
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
def get_access_names_from_dates(dates):
    """Convert dates into the directory labelling format on opendap server"""
    
    fmt_list = []
    for this_date in dates:
        for i in range(6):
            num_str = str(i).zfill(3)
            fmt_list.append('{}_{}'.format(this_date, num_str))
    return fmt_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_date_site_lists(site_file_path, data_file_path, server_file_list):
    """Get a list of sites that require data for each available date and time
       on server"""

    sites_df = get_ozflux_site_list(site_file_path)
    bool_df = pd.DataFrame(index = server_file_list)
    for site in sites_df.index:
        name = '_'.join(site.split(' '))
        f_name = '{}_ACCESS.csv'.format(name)
        if not f_name in os.listdir(data_file_path): 
            bool_df[name] = np.tile(np.nan, len(server_file_list))
            continue
        df = pd.read_csv(os.path.join(data_file_path, f_name), usecols = [0])
        dates = map(lambda x: dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'), 
                    df.date_time)
        utc_dates = convert_utc(dates, 
                                lat = sites_df.loc[site, 'Latitude'],
                                lon = sites_df.loc[site, 'Longitude'],
                                direction = 'to_utc')
        mod_dates = filter(lambda x: np.mod(x.hour, 6) == 0, utc_dates)
        str_dates = map(lambda x: dt.datetime.strftime(x, '%Y%m%d%H'), 
                        mod_dates)
        site_fmt_date_list = get_access_names_from_dates(str_dates)
        bool_df[name] = map(lambda x: x in site_fmt_date_list, server_file_list)
    d = {}
    bool_df = bool_df.T
    for col in bool_df.columns:
        l = list(bool_df[bool_df[col]==False].index)
        if l: d[col] = l
    return d
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# MAIN PROGRAM
#------------------------------------------------------------------------------

# Intialise stuff    
data_file_path = '/home/ian/Desktop/ACCESS'
url = 'http://opendap.bom.gov.au:8080/thredds/dodsC/bmrc/access-r-fc/ops/surface/'
site_file_path = '/home/ian/Temp/site_master.xls'

# Make list of time_date file names available on the opendap server
dir_list = list_opendap_dirs(url)
date_list = map(lambda x: str(x.split('/')[-2]), dir_list)
server_file_list = get_access_names_from_dates(date_list)

# Make list of time_date file names already written to existing file
list_dict = get_date_site_lists(site_file_path, data_file_path, server_file_list)

