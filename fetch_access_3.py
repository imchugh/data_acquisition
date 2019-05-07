#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 29 10:19:53 2019

@author: ian
"""

from datetime import date, timedelta
import os
import pandas as pd
from subprocess import call as spc
import xlrd
import pdb

#------------------------------------------------------------------------------
def nco_exec(site_name, date_directory, latitude, longitude):

    exec_string = ('./test.sh "{0}" "{1}" "{2}" "{3}"'
                   .format(site_name, date_directory, latitude, longitude))    
    if spc(exec_string, shell = True):
        raise RuntimeError('Error in command: {}'.format(exec_string))
    return
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

##------------------------------------------------------------------------------
#def ncks_exec(read_path, write_path, site_details, server_file_ID):
#    
#    """Build the complete ncks string and retrieve site temp file"""
#    
#    delta = 0.165
#    access_fname = os.path.join(read_path, 
#                                '{}_access.tmp'.format(server_file_ID))
#    tmp_fname = os.path.join(write_path, '{0}_{1}.tmp'.format(site, 
#                                                              server_file_ID))
#    lat_range = (str(site_details.Latitude - delta) + ',' + 
#                 str(site_details.Latitude + delta))
#    lon_range = (str(site_details.Longitude - delta) + ',' + 
#                 str(site_details.Longitude + delta))
#    ncks = ('/usr/bin/ncks -d lat,{0} -d lon,{1} {2} {3}'
#            .format(lat_range, lon_range, access_fname, tmp_fname))
#    if spc(ncks, shell = True):
#        raise RuntimeError('Error in command: {}'.format(ncks))
#    return tmp_fname
##------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def wget_exec(read_path, write_path, server_file_ID):
    
    """Build the complete wget string and retrieve temp file"""
    
    tmp_fname = os.path.join(write_path, '{}_access.tmp'.format(server_file_ID))
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
# USER PATH CONFIGURATIONS
#------------------------------------------------------------------------------

retrieval_path = 'http://opendap.bom.gov.au:8080/thredds/{}/bmrc/access-r-fc/ops/surface/'
output_path = '/home/ian/Desktop/access'
master_file_path = '/home/ian/Temp/site_master.xls'

#------------------------------------------------------------------------------
# MAIN PROGRAM
#------------------------------------------------------------------------------

site_df = get_ozflux_site_list(master_file_path) #Get site list
dirs_list = get_day_dirs()
continental_file_path = os.path.join(output_path, 'Continental_files')

for this_dir in dirs_list[1:2]:
    
    #Create local path for current month if doesn't exist (purge temp files if
    #present)
#    local_dir = os.path.join(output_path, this_dir[:6])
#    if not os.path.exists(local_dir): os.makedirs(local_dir)
#    old_tmp_files = filter(lambda x: os.path.splitext(x)[1]=='tmp', 
#                           os.listdir(local_dir))
#    map(lambda x: os.remove(x), old_tmp_files)
#    
    file_list = get_files_from_datestring(this_dir)    
    
#    for f in file_list:
#    
#        wget_exec(retrieval_path, continental_file_path, f)
        
    for site in site_df.index[0:1]:
        
        #Extract site data from temporary local access file
        
#        for f in file_list:
        try:
            site_details = site_df.loc[site]
            nco_exec(site_details.name, this_dir, site_details.Latitude,
                     site_details.Longitude)
#                tmp_fname = ncks_exec(read_path = continental_file_path,
#                                      write_path = local_dir, 
#                                      site_details = site_df.loc[site], 
#                                      server_file_ID = f)
        except RuntimeError, e:
            print e
            continue
        
        