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
import StringIO


ftp_server = 'ftp.ncdc.noaa.gov'
ftp_dir = 'pub/data/noaa/'
output_dir = '/home/ian/Temp/'


def get_data_for_site_year(usaf_num, year):
    
    def is_number(this_str):
        try:
            int(this_str)
            return True
        except ValueError:
            return False

    if not isinstance(usaf_num, (str, int, float)):
        raise TypeError('Parameter usaf_num must be str, int or float')
    usaf_num = str(int(usaf_num))
    
    if not isinstance(year, (str, int, float)):
        raise TypeError('Parameter year must be str, int or float')
    year = str(int(year))
    
    ftp = ftplib.FTP(ftp_server)
    ftp.login()
    content_list = map(lambda x: x.split('/')[-1], ftp.nlst(ftp_dir))
    years_list = sorted(filter(lambda x: is_number(x), content_list))
    if not year in years_list:
        print 'Requested year not available'
        return
    ftp.cwd(os.path.join(ftp_dir, year))
    usaf_list = map(lambda x: x.split('-')[0], ftp.nlst())
    if not usaf_num in usaf_list:
        print ('Data not available for year {0} and USAF number {1}'
               .format(year, usaf_num))
    
#    remote_file = os.path.join(ftp_dir, year, )
#    pdb.set_trace()
    
    return ftp.nlst()

def get_oz_site_list():
    
    # Set the filename
    file_name = 'isd-history.txt'
    
    # Get the data
    sio = StringIO.StringIO()
    ftp = ftplib.FTP(ftp_server)
    ftp.login()
    f_str = 'RETR {0}'.format(os.path.join(ftp_dir, file_name))
    ftp.retrbinary(f_str, sio.write)
    ftp.close()
    sio.seek(0)
    f = sio.readlines()
    
    # Format it
    l = [[0, 7], [7, 13], [13, 43], [43, 51], [51, 57], [57, 65], [65, 74],
         [74, 82], [82, 91], [91, -1]]
    header_line = 20
    data_list = []
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
    
    # Create dataframe
    df = pd.DataFrame(data_list, columns = header)
    for col in ['LAT', 'LON', 'ELEV(M)']:
        df.loc[:, col] = pd.to_numeric(df[col])
    aus_df = df[df['CTRY ST'] == 'AS']
    
    return aus_df