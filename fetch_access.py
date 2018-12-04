#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 19 14:55:15 2018

@author: ian
"""

from bs4 import BeautifulSoup
import datetime as dt
import netCDF4
import numpy as np
import os
import pandas as pd
import requests
from subprocess import call as spc
import xlrd

import pdb

#------------------------------------------------------------------------------
def check_file_dates(nc):
    """Check which files have already been seen and written"""
    
    base_date_str = getattr(nc.variables['time'], 'units')
    base_date = dt.datetime.strptime(' '.join(base_date_str.split(' ')[2:4]), 
                                     '%Y-%m-%d %H:%M:%S')
    hour = (nc.variables['time'][:].data * 24).astype(int)
    hour_mod = hour / 6 * 6
    hour_str = map(lambda x: x.zfill(3), np.mod(hour, 6).astype('str'))
    date_list = map(lambda x: base_date + dt.timedelta(hours = x), hour_mod)
    str_date_list = map(lambda x: dt.datetime.strftime(x, '%Y%m%d%H'), date_list)
    return map(lambda x: '{0}_{1}'.format(x[0], x[1]), 
               zip(str_date_list, hour_str))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def check_seen_files(site_list, server_dirs):
    """Opens existing files and cross-checks what is available on server
       against what has already been written to local file"""
    
    local_dirs = list(set(map(lambda x: x[:6], server_dirs)))
    idx = []
    for x in server_dirs: idx += get_files_from_datestring(x)
    new_df = pd.DataFrame(index = idx, columns = site_list)
    for site in new_df.columns:
        seen_files = []
        for this_dir in local_dirs:
            target = os.path.join(output_path, this_dir, '{}.nc'.format(site))
            try:
                nc = netCDF4.Dataset(target)
                seen_files += check_file_dates(nc)
            except IOError:
                pass
        new_df[site] = map(lambda x: x in seen_files, idx)
    new_df = new_df.T
    seen_file_dict = {}
    for site in new_df.columns:
        l = list(new_df[new_df[site]==False].index)
        seen_file_dict[site] = l    
    return seen_file_dict
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_files_from_datestring(datestring):
    """Turn standard datestring format for ACCESS directories into list of 
       file IDs (0-5)"""
    
    return map(lambda x: '{}_{}'.format(datestring, str(x).zfill(3)), range(6))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_ozflux_site_list(master_file_path):
    """Create a dataframe containing site names (index) and lat, long and 
       measurement interval"""
    
    wb = xlrd.open_workbook(master_file_path)
    sheet = wb.sheet_by_name('Active')
    header_row = 9
    header_list = sheet.row_values(header_row)
    df = pd.DataFrame()
    for var in ['Site', 'Latitude', 'Longitude']:
        index_val = header_list.index(var)
        df[var] = sheet.col_values(index_val, header_row + 1)   
    df.index = map(lambda x: '_'.join(x.split(' ')), df.Site)
    df.drop(header_list[0], axis = 1, inplace = True)
    return df
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
def wget_constructor(server_file_ID):
    """Build the complete wget string for retrieval"""
    
    server_dir = server_file_ID.split('_')[0]
    server_target = os.path.join(prot + svr + b_pth + server_dir,
                                'ACCESS-R_{}_surface.nc'.format(server_file_ID))
    return '{0} {1} {2}'.format(wget_prefix, tmp_path, server_target)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
prot = 'http://'
svr = 'opendap.bom.gov.au:8080'
a_pth = '/thredds/dodsC/bmrc/access-r-fc/ops/surface/'
b_pth = '/thredds/fileServer/bmrc/access-r-fc/ops/surface/'
master_file_path = '/home/ian/Temp/site_master.xls'
output_path = '/home/ian/Temp/access_nc'
if not os.path.exists(output_path): os.makedirs(output_path)
delta = 0.165
tmp_path = os.path.join(output_path, '.access.nc')
wget_prefix = '/usr/bin/wget -nv -a Download.log -O'

site_df = get_ozflux_site_list(master_file_path)
server_dirs = list_opendap_dirs(prot + svr + a_pth)
seen_file_dict = check_seen_files(site_df.index, server_dirs)

for server_dir in server_dirs[2:3]:
    local_dir = os.path.join(output_path, server_dir[:6])
    if not os.path.exists(local_dir): os.makedirs(local_dir)
    server_file_list = get_files_from_datestring(server_dir)
    for server_file_ID in server_file_list:
        site_list = seen_file_dict[server_file_ID]
        if len(site_list) == 0:
            print 'Data already appended: skipping {}'.format(server_file_ID)
            continue
        cmd = wget_constructor(server_file_ID)
        if spc(cmd, shell=True):
            print 'Error in command: ', cmd
            continue
        print 'Extracting data for date {} for site: '.format(server_file_ID)
        for site in site_list:
            print site,
            existing_fname = os.path.join(local_dir, '{}.nc'.format(site))
            tmp_fname = os.path.join(local_dir, '.{}.nc'.format(site))
            lat = site_df.loc[site, 'Latitude']
            lon = site_df.loc[site, 'Longitude']
            lat_range = str(lat - delta) + ',' + str(lat + delta)
            lon_range = str(lon - delta) + ',' + str(lon + delta)
            ncks = ('/usr/bin/ncks -d lat,{0} -d lon,{1} {2} {3}'
                    .format(lat_range, lon_range, tmp_path, tmp_fname))
            if spc(ncks, shell = True):
                print 'Error in command: ', ncks
            if not os.path.exists(existing_fname):
                os.rename(tmp_fname, existing_fname)
            else:
                ncrcat = (r'/usr/bin/ncrcat --rec_apn {0} {1}'
                          .format(os.path.join(local_dir, tmp_fname), 
                                  os.path.join(local_dir, existing_fname)))
                if spc(ncrcat, shell=True):
                    print 'Error in command: ', ncrcat
                os.remove(tmp_fname)
        os.remove(tmp_path)
        print   

print ' --- All done ---'