#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue May 15 16:08:18 2018

@author: ian
"""

import ftplib
import os
import StringIO
import zipfile
import pdb

ftp_server = 'ftp.bom.gov.au'
ftp_dir = 'anon2/home/ncc/srds/Scheduled_Jobs/DS010_OzFlux/'
search_str = 'AWS' # 'globalsolar'

#------------------------------------------------------------------------------
def get_file_list():

    ftp = ftplib.FTP(ftp_server)
    ftp.login()    
    zf_list = ftp.nlst(ftp_dir)
    name_list = []
    for this_file in zf_list:
        if not search_str in this_file: continue
        f_str = 'RETR {0}'.format(this_file)
        sio = StringIO.StringIO()
        ftp.retrbinary(f_str, sio.write)
        sio.seek(0)
        zf = zipfile.ZipFile(sio)
        name_list = name_list + zf.namelist()
    ftp.close()
    return name_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def check_list_integrity(id_list):
    
    return_list = []
    if id_list:
        if not isinstance(id_list, list):
            raise TypeError('id_list argument must be a list!')
        for item in id_list:
            if not isinstance(item, (int, str)):
                raise TypeError('each item in list must be either integer or '
                                'string representation thereof! You passed {} '
                                'in list'.format(type(item)))
            if isinstance(item, int): return_list.append(str(item).zfill(6))
            if isinstance(item, str):
                int(item)
                try:
                    assert len(item) <= 6
                except AssertionError:
                    raise RuntimeError('BOM station IDs have no more than six '
                                       'numbers! You passed {} in list'
                                       .format(type(item)))
                return_list.append(item.zfill(6))
        return return_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def subset_station_list(files_list, target_ID_list):
    
    unq_files_list = sorted(list(set([f for f in files_list if 'Data' in f])))
    avail_list = [f.split('_')[2] for f in unq_files_list]
    common_list = list(set(avail_list).intersection(target_ID_list))
    file_name_list = []
    for f in common_list:
        idx = avail_list.index(f)
        file_name_list.append(unq_files_list[idx])
    return file_name_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_ftp_data(id_list = None):

    # Check valid list was passed
    id_list = check_list_integrity(id_list)
    
    # Login to ftp server      
    ftp = ftplib.FTP(ftp_server)
    ftp.login()   
    
    # Open the separate zip files and combine in a single zip file 
    # held in dynamic memory - ignore the solar data
    master_sio = StringIO.StringIO() 
    master_zf = zipfile.ZipFile(master_sio, 'w')
    zip_file_list = [os.path.split(f)[1] for f in ftp.nlst(ftp_dir)]   
    for this_file in zip_file_list:
        if 'globalsolar' in this_file: continue
        in_file = os.path.join(ftp_dir, this_file)
        f_str = 'RETR {0}'.format(in_file)
        sio = StringIO.StringIO()
        ftp.retrbinary(f_str, sio.write)
        sio.seek(0)
        zf = zipfile.ZipFile(sio)
        file_list = subset_station_list(zf.namelist(), id_list)
        for f in file_list:
            master_zf.writestr(f, zf.read(f))
        zf.close()
    ftp.close()

    return master_zf
#------------------------------------------------------------------------------