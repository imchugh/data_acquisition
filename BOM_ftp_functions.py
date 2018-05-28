#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue May 15 16:08:18 2018

@author: ian
"""

import ftplib
import StringIO
import zipfile

ftp_server = 'ftp.bom.gov.au'
ftp_dir = 'anon2/home/ncc/srds/Scheduled_Jobs/DS010_OzFlux/'
search_str = 'AWS' # 'globalsolar'

ftp = ftplib.FTP(ftp_server)
ftp.login()

zf_list = ftp.nlst(ftp_dir)
master_sio = StringIO.StringIO() 
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