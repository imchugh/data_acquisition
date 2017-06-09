#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jun  5 11:42:32 2017

@author: ian
"""

import ftplib
import os
import zipfile
import xlrd
import pdb
import timeit

def subset_station_list(files_list, target_ID_list):
    
    unq_files_list = list(set(files_list))
    unq_files_list.sort()
    unq_target_ID_list = list(set(target_ID_list))
    unq_target_ID_list.sort()
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

    # Check directories exist and make them if not
    if not os.path.isdir(output_dir): os.mkdir(output_dir)
    for sub in ['met_obs', 'solar_obs']:
        sub_dir = os.path.join(output_dir, sub)
        if not os.path.isdir(sub_dir):
            os.mkdir(sub_dir)
    
    # Login to ftp server         
    ftp = ftplib.FTP(ftp_server)
    ftp.login()
    
    # grab the zip files - separate the solar and met data
    ftp_file_list = [os.path.split(this_file)[1] 
                     for this_file in ftp.nlst(ftp_dir)]    
    for this_file in ftp_file_list:
        ext_dir = 'met_obs' if 'AWS' in this_file else 'solar_obs'
        out_dir = os.path.join(output_dir, ext_dir)
        out_file = os.path.join(out_dir, this_file)
        if not os.path.isfile(out_file):
            in_file = os.path.join(ftp_dir, this_file)
            with open(out_file, 'w') as f:          
                f_str = 'RETR {0}'.format(in_file)
                ftp.retrbinary(f_str, f.write)   
                print f_str
        zip_obj = zipfile.ZipFile(out_file)
        file_list = zip_obj.namelist()
        file_list.sort()
        file_list = subset_station_list(file_list, req_file_list)
        for f in file_list:
            if not os.path.isfile(os.path.join(out_dir, f)):
                zip_obj.extract(f, out_dir)
        zip_obj.close()
            
    ftp.close()
    
    return

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
    str_list = list(set(str_list))
    str_list.sort()
    ID_list = [this.split(',')[0] for this in str_list]
    name_list = [this.split(',')[1] for this in str_list]
    
    return ID_list, name_list

#------------------------------------------------------------------------------
ftp_server = 'ftp.bom.gov.au'
ftp_dir = 'anon2/home/ncc/srds/Scheduled_Jobs/DS010_UWA-SEE/'
output_dir = '/home/ian/BOM_data'
sites_file = '/home/ian/Temp/AWS_Locations.xls'

BOM_ID_list, BOM_name_list = get_station_list(sites_file, 'OzFlux')

#get_ftp_data(ftp_server, ftp_dir, output_dir, BOM_ID_list)

test=os.listdir('/home/ian/BOM_data')
test=[i for i in test if 'Data' in i]
test.sort()
a=[i.split('_')[2] for i in test]