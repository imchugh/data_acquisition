#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon May 14 10:15:05 2018

@author: ian
"""

import ftplib
import os
import pandas as pd
import pdb

ftp_server = 'ftp.ncdc.noaa.gov'
ftp_dir = 'pub/data/noaa/'
output_dir = '/home/ian/Temp/'
file_name = 'isd-history.txt'

ftp = ftplib.FTP(ftp_server)
ftp.login()
file_list = ftp.nlst(ftp_dir)
f_str = 'RETR {0}'.format(os.path.join(ftp_dir, file_name))
with open(os.path.join(output_dir, file_name), 'wb') as f:
    ftp.retrbinary(f_str, f.write, 8*1024)
ftp.close()

l = [[0, 7], [7, 13], [13, 43], [43, 51], [51, 57], [57, 65], [65, 74],
     [74, 82], [82, 91], [91, -1]]
float_cols = [5, 7]

header_line = 20
data_list = []
counter = 0
with open(os.path.join(output_dir, file_name)) as f:
    for i, line in enumerate(f):
        if i < header_line: continue
        if i == header_line: 
            header = line
            continue
        if len(line) == 100:
            data_list.append(map(lambda x: (line[x[0]: x[1]]).strip(), l))
header = header.split()
header = (header[:2] + [' '.join([header[2], header[3]])] + 
                       [' '.join([header[4], header[5]])] + header[6:])        

df = pd.DataFrame(data_list, columns = header)
for col in ['LAT', 'LON', 'ELEV(M)']:
    df.loc[:, col] = pd.to_numeric(df[col])
    
aus_df = df[df['CTRY ST'] == 'AS']