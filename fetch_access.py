#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Nov 19 14:55:15 2018

@author: ian
"""

import os, sys
from subprocess import call as spc
from datetime import date, timedelta
from bs4 import BeautifulSoup
import datetime as dt
import numpy as np
import pandas as pd
import requests
import xlrd

import pdb

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
    return map(lambda x: 'ACCESS-R_{0}_{1}_surface.nc'.format(x[0], x[1]), 
               zip(str_date_list, hour_str))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------

prot = 'http://'
svr = 'opendap.bom.gov.au:8080'
a_pth = '/thredds/dodsC/bmrc/access-r-fc/ops/surface/'
b_pth = '/thredds/fileServer/bmrc/access-r-fc/ops/surface/'
master_file_path = '/home/ian/Temp/site_master.xls'
output_path = '/home/ian/Temp/access_nc'

current_dirs = list_opendap_dirs(prot + svr + a_pth)

site_df = get_ozflux_site_list(master_file_path)

delta = 0.165

tmp_path = os.path.join(output_path, '.access.nc')
wget = '/usr/bin/wget -nv -a Download.log -O'

if not os.path.exists(output_path): os.makedirs(output_path)

for this_dir in current_dirs:
    local_dir = os.path.join(output_path, this_dir[:6])
    if not os.path.exists(local_dir): os.makedirs(local_dir)        
    for i in range(6):
        target_fname = 'ACCESS-R_{0}_{1}_surface.nc'.format(this_dir, 
                                                            str(i).zfill(3))
        target_path = os.path.join(prot + svr + b_pth + this_dir, target_fname)
        cmd = '{0} {1} {2}'.format(wget, tmp_path, target_path)
        if spc(cmd, shell=True):
            print 'Error in command: ', cmd
        else:
            for site in [site_df.index[0]]:
                site_name = site.replace(' ', '_')
                existing_fname = os.path.join(local_dir, '{}.nc'.format(site_name))
                tmp_fname = os.path.join(local_dir, '.{}.nc'.format(site_name))
                lat = site_df.loc[site, 'Latitude']
                lon = site_df.loc[site, 'Longitude']
                print site_name, lat, lon
                lat_range = str(lat - delta) + ',' + str(lat + delta)
                lon_range = str(lon - delta) + ',' + str(lon + delta)
                ncks = ('/usr/bin/ncks -d lat,{0} -d lon,{1} {2} {3}'
                        .format(lat_range, lon_range, tmp_path, tmp_fname))
                if spc(ncks, shell = True):
                    print 'Error in command: ', ncks
                if not os.path.exists(existing_fname):
                    os.rename(tmp_fname, existing_fname)
                else:
                    ncrcat = (r'/usr/bin/ncrcat -rec_apn {0} {1}'
                              .format(os.path.join(local_dir, existing_fname), 
                                      os.path.join(local_dir, tmp_fname)))
                    if spc(ncrcat, shell=True):
                        pdb.set_trace()
                        print 'Error in command: ', ncrcat
                    else:
                        print 'Tick'
                    os.remove(tmp_fname)
            os.remove(tmp_path)
#    for site in site_df.index:
#        sname = site.replace(' ', '_')
#        site_out_path = os.path.join(local_dir, sname)
#        ncrcat = (r'/usr/bin/ncrcat {0}_{1}_00[012345].nc {2}.nc'
#                  .format(os.path.join(local_dir, sname), this_dir, 
#                          site_out_path))
#        if spc(ncrcat, shell=True):
#            print 'Error in command: ', ncrcat
#        else:
#            print 'Tick'    

concat_file_list = map(lambda x: '{}.nc'.format(x), site_df.index)
for f in concat_file_list:
    target = os.path.join(output_path, '201811', f)
    nc = netCDF4.Dataset(target)
    seen_files = check_file_dates(nc)
    print ('The following files have been concatenated to {0}: {1}'
           .format(f, ', '.join(seen_files)))

print ' --- All done ---'


 
#l = 

######

#
#prot = 'http://'
#svr = 'opendap.bom.gov.au:8080'
#pth = '/thredds/fileServer/bmrc/access-r-fc/ops/surface/'
#
#tmpfile = './temp/access.nc'
#site_list = 'sites_list.txt'
#
## check if date is on command line - else default to yesterday
#args = sys.argv[1:]
#if len(args) < 1:
#    yest = date.today() - timedelta(1)
#    ymd = yest.strftime('%Y%m%d')
#else:
#    ymd = args[0]
#
##print ymd
##exit(0)
#
##ymd = '20150514'
#ym = ymd[:6]
#hrs = ['00', '06', '12', '18']
#pref = '/ACCESS-R_' 
#suff = '_surface.nc'
#
#
## create destination dir
#if not os.path.exists(ym):
#    print 'Creating dir: ', ym
#    os.makedirs(ym)
#
## formulate wget command
##wget = '/usr/bin/wget -nv -N -nH -np -a Download.log -P ' + ym + ' -0 ' + tmpfile + ' '
#wget = '/usr/bin/wget -nv -a Download.log -O ' + tmpfile + ' '
#
## read in site lat/lon
#with open(site_list, 'r') as fin:
#    sites = list(fin)
#
######
#for h in hrs:
#    for t in range(7):
#        access = pref + ymd + h + '_' + str(t).zfill(3) + suff
#        cmd = wget + prot + svr + pth + ymd + h + access
#        print cmd
#        if spc(cmd, shell=True):
#            print 'Error in command: ', cmd
#        else:
#            # extract data for the sites, then can throw away the tmpfile
#        #if True:
#            for sl in sites:
#                sname, slat, slon = sl.split(',')
#                sname = sname.replace(' ', '_')
#                lat = float(slat)
#                lon = float(slon.strip())
#                #print sname, lat, lon
#                ncks = '/usr/bin/ncks -d lat,' + str(lat - delta) + ',' + str(lat + delta)  + ' -d lon,' \
#                     + str(lon - delta) + ',' + str(lon + delta)  + ' ' \
#                     + tmpfile + ' ' + ym + '/' + sname + '_' + ymd + h + '_' + str(t).zfill(3) + '.nc'
#                print ncks
#                if spc(ncks, shell=True):
#                    print 'Error in command: ', ncks
#            # 
#            os.remove(tmpfile)
# 
#
#print ' --- All done ---'
# 
