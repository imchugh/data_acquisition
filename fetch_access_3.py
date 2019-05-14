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
def nco_exec(site_name, date_directory, latitude, longitude):

    """Call the shell script that cuts out the site coordinates and 
       concatenates with existing data (using NCO)"""
    
    exec_string = ('./test.sh "{0}" "{1}" "{2}" "{3}"'
                   .format(site_name, date_directory, latitude, longitude))    
    if spc(exec_string, shell = True):
        raise RuntimeError('Error in command: {}'.format(exec_string))
    return
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def purge_dir(directory, file_ext = '.tmp'):
    
    """Dump any files not required"""
    
    f_list = filter(lambda x: os.path.splitext(x)[1] == '.tmp', 
                    os.listdir(directory))
    for f in [os.path.join(directory, x) for x in f_list]:
        os.remove(f)
#------------------------------------------------------------------------------

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

site_df = get_ozflux_site_list(master_file_path)
dirs_list = get_day_dirs()
continental_file_path = os.path.join(output_path, 'Continental_files')

# Pre-purge the continental file path for all temp files
purge_dir(continental_file_path)

# For each six-hour directory...
for this_dir in dirs_list:
    
    # Get a list of the files we want to extract (UTC + 0-7)
    file_list = get_files_from_datestring(this_dir)

    # Grab the continent-wide file        
    for f in file_list:    
        wget_exec(retrieval_path, continental_file_path, f)
    
    # Cut out site from continent-wide file and append (see shell script)    
    for site in site_df.index:
        
        try:
            site_details = site_df.loc[site]
            nco_exec(site_details.name, this_dir, site_details.Latitude,
                     site_details.Longitude)
        except RuntimeError, e:
            print e
            continue