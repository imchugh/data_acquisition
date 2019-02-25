#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 19 14:55:15 2018

@author: ian
"""

from bs4 import BeautifulSoup
import datetime as dt
from datetime import date, timedelta
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
def get_day_dirs():
    
    yest = date.today() - timedelta(1)
    ymd = yest.strftime('%Y%m%d')
    return map(lambda x: ymd + x, ['00', '06', '12', '18'])
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_files_from_datestring(datestring):
    """Turn standard datestring format for ACCESS directories into list of 
       file IDs (0-5)"""
    
    return map(lambda x: '{}_{}'.format(datestring, str(x).zfill(3)), range(7))
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
def list_opendap_dirs(url):
    """Scrape list of directories from opendap surface url"""
    
#    pdb.set_trace()
    full_url = url.format('dodsC')
    page = requests.get(full_url).text
    soup = BeautifulSoup(page, 'html.parser')    
    dir_list = [url + '/' + node.get('href') for node in soup.find_all('a') 
                if node.get('href').endswith('html')]
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
def ncks_exec(write_path, site_series, server_file_ID):
    """Build the complete ncks string and retrieve site temp file"""
    
    delta = 0.165
    tmp_fname = os.path.join(write_path, '{0}_{1}.tmp'.format(site, server_file_ID))
    access_fname = os.path.join('/'.join(write_path.split('/')[:-1]), 
                                '.access.tmp')
    lat_range = (str(site_series.Latitude - delta) + ',' + 
                 str(site_series.Latitude + delta))
    lon_range = (str(site_series.Longitude - delta) + ',' + 
                 str(site_series.Longitude + delta))
    ncks = ('/usr/bin/ncks -d lat,{0} -d lon,{1} {2} {3}'
            .format(lat_range, lon_range, access_fname, tmp_fname))
    if spc(ncks, shell = True):
        raise RuntimeError('Error in command: {}'.format(ncks))
    return tmp_fname
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def ncrcat_exec(existing_fname, new_fname):
    """Will alter this to use rec_apn if can resolve - update: NCO issue,
       developer expects resolution in 4.7.9 (January 2018)"""
    
    alt_fname = os.path.join(os.path.dirname(existing_fname), 
                             '{}.tmp'.format(os.path.basename(existing_fname)))
    os.rename(existing_fname, alt_fname)
    ncrcat = (r'/usr/bin/ncrcat -O {0} {1} {2}'.format(alt_fname, new_fname, 
                                                       existing_fname))
    if spc(ncrcat, shell=True):
        print 'Error in command: ', ncrcat
    os.remove(alt_fname)
    os.remove(new_fname)
    return
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def wget_exec(read_path, write_path, server_file_ID):
    """Build the complete wget string and retrieve temp file"""
    
    tmp_fname = os.path.join(write_path, '.access.tmp')
    wget_prefix = '/usr/bin/wget -nv -a Download.log -O'
    server_dir = server_file_ID.split('_')[0]
    full_read_path = read_path.format('fileServer') + server_dir
    server_fname = os.path.join(full_read_path,
                                'ACCESS-R_{}_surface.nc'.format(server_file_ID))
    cmd = '{0} {1} {2}'.format(wget_prefix, tmp_fname, server_fname)
    if spc(cmd, shell=True):
        raise RuntimeError('Error in command: {}'.format(cmd))
    return tmp_fname
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
#Initialise    
retrieval_path = 'http://opendap.bom.gov.au:8080/thredds/{}/bmrc/access-r-fc/ops/surface/'
master_file_path = '/home/ian/Temp/site_master.xls'
output_path = '/home/ian/Temp/access_nc'
if not os.path.exists(output_path): os.makedirs(output_path)

# Do preliminary checks
site_df = get_ozflux_site_list(master_file_path) #Get site list
server_dirs = get_day_dirs()
#server_dirs = list_opendap_dirs(retrieval_path) #Get available opendap dirs
seen_file_dict = check_seen_files(site_df.index, server_dirs) #Cross check data

#For each six-hourly directory...
for server_dir in server_dirs: 

    #Create local path for current month if doesn't exist (purge temp files if
    #present)
    local_dir = os.path.join(output_path, server_dir[:6])
    if not os.path.exists(local_dir): os.makedirs(local_dir)
    old_tmp_files = filter(lambda x: os.path.splitext(x)[1]=='tmp', 
                           os.listdir(local_dir))
    map(lambda x: os.remove(x), old_tmp_files)
    pdb.set_trace()    
    #For each hourly file in list of files in this directory...
    server_file_list = get_files_from_datestring(server_dir)
    for server_file_ID in server_file_list: 
        
        # Get the sites (if any) requiring data from this file (or next if not)
        site_list = seen_file_dict[server_file_ID]
        if len(site_list) == 0:
            print 'Data already appended: skipping {}'.format(server_file_ID)
            continue
        
        #Write a temporary local access file (containing all Oz data for a 
        #single time step) - continue if fails
        try: 
            master_tmp_fname = wget_exec(read_path = retrieval_path, 
                                         write_path = output_path, 
                                         server_file_ID = server_file_ID)
        except RuntimeError, e: 
            print e
            continue
        
        #Iterate through sites
        print 'Extracting data for date {} for site: '.format(server_file_ID)
        for site in site_list[:1]:
            print site,
            
            #Extract site data from temporary local access file
            try:
                tmp_fname = ncks_exec(write_path = local_dir, 
                                      site_series = site_df.loc[site], 
                                      server_file_ID = server_file_ID)
            except RuntimeError, e:
                print e
                continue

#            #Check if there is an existing site file; if not, rename 
#            #temporary file, otherwise append to the existing
#            existing_fname = os.path.join(local_dir, '{}.nc'.format(site))
#            if not os.path.exists(existing_fname):
#                os.rename(tmp_fname, existing_fname)        
#            else:
#                ncrcat_exec(existing_fname, tmp_fname)

        os.remove(master_tmp_fname)
        print   

print ' --- All done ---'