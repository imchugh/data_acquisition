#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Jun  6 14:40:23 2017

@author: ian
"""

import ftplib
import os
import zipfile
import StringIO
import pandas as pd
import datetime as dt
import xlrd

# Grab the list of stations from the excel file
def get_station_list(sites_file, sheet_name):

    xl_obj = xlrd.open_workbook(sites_file)
    xl_sheet = xl_obj.sheet_by_name(sheet_name)
    cols_dict = {'ID': [i for i, ID in enumerate(xl_sheet.row(9)) 
                        if ID.value == 'BoM ID'],
                 'name': [i for i, ID in enumerate(xl_sheet.row(9)) 
                          if ID.value == 'Name']}
    if not len(cols_dict['ID']) == len(cols_dict['name']):
        raise IOError('Error in sites file: number of columns containing site '
                      'names must equal number of columns containing site '
                      'IDs! Exiting')
    master_list = []
    for var in ['ID', 'name']:
        this_list = []
        cols_list = cols_dict[var]
        for i in cols_list:
            this_list.append([this.value for this in 
                              xl_sheet.col_slice(i, 10)])
        master_list.append([item for sublist in this_list for item in sublist])
    temp_list = zip(master_list[0], master_list[1])
    str_list = []
    for this_tuple in temp_list:
        try:       
            a = str(int(this_tuple[0])).zfill(6)
            b = this_tuple[1]
            str_list.append(','.join([a, b]))
        except:
            continue
    str_list = sorted(list(set(str_list)))
    ID_list = [this.split(',')[0] for this in str_list]
    name_list = [this.split(',')[1] for this in str_list]
    
    return ID_list, name_list

def subset_station_list(files_list, target_ID_list):
    
    unq_files_list = sorted(list(set(files_list)))
    unq_target_ID_list = sorted(list(set(target_ID_list)))
    f_names_list = []
    counter = 0
    for ID in unq_target_ID_list:
        for f_name in unq_files_list[counter:]:
            if ID in f_name:
                f_names_list.append(f_name)
                counter = unq_files_list.index(f_name)
                break
                
    return f_names_list

def get_ftp_data(ftp_server, ftp_dir, output_dir, req_file_list):

    # Login to ftp server         
    ftp = ftplib.FTP(ftp_server)
    ftp.login()
    
    # Check directories exist and make them if not
    if not os.path.isdir(output_dir): os.mkdir(output_dir)
        
    # Open the zip files and unzip to directory - ignore the solar data
    master_file_list = []
    zip_file_list = [os.path.split(f)[1] for f in ftp.nlst(ftp_dir)]    
    for this_file in zip_file_list:
        if 'globalsolar' in this_file: continue
        in_file = os.path.join(ftp_dir, this_file)
        f_str = 'RETR {0}'.format(in_file)
        sio = StringIO.StringIO()
        ftp.retrbinary(f_str, sio.write)
        sio.seek(0)
        zip_obj = zipfile.ZipFile(sio)
        file_list = zip_obj.namelist()
        file_list = subset_station_list(file_list, req_file_list)
        master_file_list = master_file_list + file_list
        for f in file_list:
            if not os.path.isfile(os.path.join(output_dir, f)):
                zip_obj.extract(f, output_dir)
        zip_obj.close()
    
    # Check for differences between requested site files and available site
    # files and report to user (print to screen)
    returned_file_list = [f.split('_')[2] for f in master_file_list]
    missing_file_list = list(set(req_file_list) - set(returned_file_list))
    print ('The following site IDs were not available: {0}'
           .format(', '.join(missing_file_list)))
       
    ftp.close()

    return

###############################################################################
# MAIN PROGRAM #
###############################################################################
ftp_server = 'ftp.bom.gov.au'
ftp_dir = 'anon2/home/ncc/srds/Scheduled_Jobs/DS010_UWA-SEE/'
output_dir = '/home/ian/BOM_data'
sites_file = '/home/ian/Temp/AWS_Locations.xls'

BOM_ID_list, BOM_name_list = get_station_list(sites_file, 'OzFlux')
#get_ftp_data(ftp_server, ftp_dir, output_dir, BOM_ID_list)

f_list = os.listdir(output_dir)
test_f = os.path.join(output_dir, f_list[0])
df = pd.read_csv(test_f)
var_list = df.columns
df.index = [dt.datetime(df.loc[i, var_list[3]],
                        df.loc[i, var_list[4]],
                        df.loc[i, var_list[5]],
                        df.loc[i, var_list[6]],
                        df.loc[i, var_list[7]]) for i in range(len(df))]
test_dates = pd.date_range(df.index[0], df.index[-1], freq = '30T')
df = df.reindex(test_dates)