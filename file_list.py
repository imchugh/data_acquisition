#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 30 17:17:22 2018

@author: ian
"""

from bs4 import BeautifulSoup
import datetime as dt
import os
import pandas as pd
import pdb
import requests
import xlrd

def is_data_file(file_str):
    
    sub_str = file_str.split('/')[-2]
    try:
        dt.datetime.strptime(sub_str, '%Y%m%d%H')
        return True
    except:
        return False

def list_opendap_dirs(url, ext = 'html'):
    
    page = requests.get(url).text
    soup = BeautifulSoup(page, 'html.parser')    
    l = [url + '/' + node.get('href') for node in soup.find_all('a') 
         if node.get('href').endswith(ext)]
    return filter(lambda x: is_data_file(x), l)

def get_ozflux_site_list(master_file_path):
    
    wb = xlrd.open_workbook(master_file_path)
    sheet = wb.sheet_by_name('Active')
    header_row = 9
    header_list = sheet.row_values(header_row)
    df = pd.DataFrame()
    for var in ['Site', 'Latitude', 'Longitude']:
        index_val = header_list.index(var)
        df[var] = sheet.col_values(index_val, header_row + 1)   
    df.index = df[header_list[0]]
    df.drop(header_list[0], axis = 1, inplace = True)
    return df

#------------------------------------------------------------------------------
# MAIN PROGRAM
#------------------------------------------------------------------------------
    
import_dir = '/home/ian/Desktop/ACCESS'
url = 'http://opendap.bom.gov.au:8080/thredds/dodsC/bmrc/access-r-fc/ops/surface/'
site_file_path = '/home/ian/Temp/site_master.xls'

sites = get_ozflux_site_list(site_file_path)
    
dir_list = list_opendap_dirs(url)
date_list = map(lambda x: str(x.split('/')[-2]), dir_list)
date_file_dict = {}
for this_date in date_list:
    l = []
    for i in range(6):
        num_str = str(i).zfill(3)
        l.append('{}_{}'.format(this_date, num_str))
    date_file_dict[this_date] = l

data_dict = {}
for f_name in os.listdir(import_dir):
    name = '_'.join(f_name.split('_')[:-1])
    df = pd.read_csv(os.path.join(import_dir, f_name))
    py_dates = map(lambda x: dt.datetime.strptime(x, '%Y-%m-%d %H:%M:%S'), 
                   df.date_time)
    tz = 
    str_dates = map(lambda x: dt.datetime.strftime(x, '%Y%m%d%H'), py_dates)
    data_dict[name] = str_dates