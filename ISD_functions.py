#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon May 14 10:15:05 2018

@author: ian
"""

import datetime as dt
import ftplib
import gzip
import numpy as np
import os
import pandas as pd
import pdb
import StringIO


ftp_server = 'ftp.ncdc.noaa.gov'
ftp_dir = 'pub/data/noaa/'
output_dir = '/home/ian/Temp/'

#------------------------------------------------------------------------------
def format_dict():
    
    names_list = ['variable_characters', 'usaf_id', 'wban_id', 'date', 'time',
                  'source_flag', 'latitude', 'longitude', 'report_type',
                  'elevation', 'call_letter', 'qc_process', 'wind_direction',
                  'wind_direction_qc', 'wind_observation_type', 'wind_speed',
                  'wind_speed_qc', 'air_temperature', 'air_temperature_qc',
                  'dew_point_temperature', 'dew_point_temperature_qc',
                  'air_pressure', 'air_pressure_qc']

    tuple_list = [(0, 3), (4, 9), (10, 14), (15, 22), (23, 26), (27, 27),
                  (28, 33), (34, 40), (41, 45), (46, 50), (51, 55), (56, 59),
                  (60, 62), (63, 63), (64, 64), (65, 68), (69, 69), (87, 91),
                  (92, 92), (93, 97), (98, 98), (99, 103), (104, 104)]
    
    return names_list, tuple_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def power_scaling_dict():
    
    return {'latitude': 3, 'longitude': 3, 'wind_speed': 1, 
            'air_temperature': 1, 'dew_point_temperature': 1, 
            'air_pressure': 1}
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_data_for_site_year(usaf_num, year):
    
    def is_number(this_str):
        try:
            int(this_str)
            return True
        except ValueError:
            return False

    if not isinstance(usaf_num, str):
        raise TypeError('Parameter usaf_num must be str')
    
    if not isinstance(year, str):
        raise TypeError('Parameter year must be str')
    
    ftp = ftplib.FTP(ftp_server)
    ftp.login()
    content_list = map(lambda x: x.split('/')[-1], ftp.nlst(ftp_dir))
    years_list = sorted(filter(lambda x: is_number(x), content_list))
    if not year in years_list:
        print 'Requested year not available'
        return
    ftp.cwd(os.path.join(ftp_dir, year))
    usaf_list = ftp.nlst()
    ID_list = map(lambda x: x.split('-')[0], usaf_list)
    if not usaf_num in ID_list:
        print ('Data not available for year {0} and USAF number {1}'
               .format(year, usaf_num))
        return
    idx = ID_list.index(usaf_num)
    remote_file = os.path.join('/', ftp_dir, year, usaf_list[idx])
    with open('/home/ian/Desktop/test.gz', 'wb') as file_handle:
        ftp.retrbinary('RETR {}'.format(remote_file), file_handle.write)
    ftp.close()
    return
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def uncompress_gzip(fname):
    
    vars_list, tuple_list = format_dict()
    ref_dict = dict(zip(vars_list, tuple_list))
    test = gzip.open(fname, 'rb')
    master_list = []
    for line in test:
        sub_list = []
        for var in vars_list:
            char_nums = ref_dict[var]
            sub_list.append(line[char_nums[0]: char_nums[1] + 1])
        master_list.append(sub_list)
    data_array = np.array(master_list)
    df = pd.DataFrame()
    for i, var in enumerate(vars_list):
        try:
            func = converters(var)
            df[var] = map(lambda x: func(x), data_array[:, i])
        except KeyError:
            df[var] = data_array[:, i]
    df.index = map(lambda x: dt.datetime.combine(x[0], x[1]),
                   zip(df.date, df.time))   
    return df.resample('60T').interpolate()
#------------------------------------------------------------------------------

def converters(var):
    
    d = {'date': (lambda x: dt.datetime.strptime(x, '%Y%m%d').date()),
         'time': (lambda x: dt.datetime.strptime(x, '%H%M').time()),
         'latitude': (lambda x: int(x) / 10.0**3), 
         'longitude': (lambda x: int(x) / 10.0**3),
         'wind_speed': (lambda x: int(x) / 10.0), 
         'wind_direction': (lambda x: int(x) / 10.0),
         'air_temperature': (lambda x: int(x) / 10.0), 
         'air_pressure': (lambda x: int(x) / 10.0)}
    
    return d[var]
    
#------------------------------------------------------------------------------
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
    
    return df#aus_df