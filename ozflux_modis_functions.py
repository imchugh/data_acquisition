#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed May  9 14:06:55 2018

@author: ian
"""

import datetime as dt
import numpy as np
import os
import pandas as pd
import xlrd
import pdb

import modis_functions as mf
reload(mf)       

#------------------------------------------------------------------------------
def check_dir(dir_path):
    if not os.path.isdir(dir_path): os.mkdir(dir_path)
#------------------------------------------------------------------------------
  
#------------------------------------------------------------------------------
def get_bands_to_process(product, drop_bands_dict):
    
    bands = mf.get_band_list(product)
    if product in drop_bands_dict.keys():
        for band in drop_bands_dict[product]: bands.remove(band)
    if '11' in product:
        for band in ['day', 'night']:
            qc_band = mf.get_qc_variable_band(product, band)
            bands.remove(qc_band)
    else:
        qc_band = mf.get_qc_variable_band(product)
        if not qc_band is None: bands.remove(qc_band)
    return bands
#------------------------------------------------------------------------------
  
#------------------------------------------------------------------------------
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

#------------------------------------------------------------------------------    
def read_from_existing_file(target_file_path):

    with open(target_file_path) as f:
        header_list = []
        for i, line in enumerate(f):
            if line.split(',')[0] == 'Date': break
            header_list.append(line)
    df = pd.read_csv(target_file_path, skiprows = range(i))
    df.index = map(lambda x: dt.datetime.strptime(x, '%Y-%m-%d').date(), df.Date)
    df.drop('Date', axis = 1, inplace = True)
    return df
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# Main program
#------------------------------------------------------------------------------

master_file_path = '/mnt/OzFlux/Sites/site_master.xls'
output_path = '/rdsi/market/MODIS'
drop_bands_dict = {'MCD12Q1': ['LC_Property_1', 'LC_Property_2', 'LC_Property_3',
                               'Land_Cover_Type_1_Secondary', 
                               'Land_Cover_Type_1_Secondary_Percent']}

product_list = mf.get_product_list()
site_df = get_ozflux_site_list(master_file_path)

# Iterate over sites and check dir exists (make if not)
for site in ['Adelaide River']:#site_df.index:
    site_name = '_'.join(site.split(' '))
    site_dir_path = os.path.join(output_path, site_name)
    check_dir(site_dir_path)
    lat, lon = site_df.loc[site, 'Latitude'], site_df.loc[site, 'Longitude']
    
    # Iterate over products and check dir exists (make if not) and get 
    # available dates
    for product in ['MOD11A2']:#product_list:
        band_list = get_bands_to_process(product, drop_bands_dict)
        product_site_dir_path = os.path.join(site_dir_path, product)
        check_dir(product_site_dir_path)
        product_dates_available = map(lambda x: 
                                      dt.datetime.strptime(x[1:], '%Y%j').date(),
                                      mf.get_date_list(lat, lon, product))
        
        # Iterate over bands    
        for band in band_list:
            file_name = '{0}_{1}_{2}.csv'.format(site_name, product, band)
            path_filename = os.path.join(product_site_dir_path, file_name)
            if os.path.isfile(path_filename):
                df = read_from_existing_file(path_filename)
                dates_to_retrieve = sorted(filter(lambda x: not x in df.index,
                                                  product_dates_available))
                if not dates_to_retrieve: continue
                x = mf.modis_data_by_npixel(lat, lon, product, band, 
                                            start_date = dates_to_retrieve[0],
                                            end_date = dates_to_retrieve[-1],
                                            site = site)
            else:
                x = mf.modis_data_by_npixel(lat, lon, product, band, site = site)
            if x.valid_rows == 0: continue
            x.write_to_file(product_site_dir_path)
#------------------------------------------------------------------------------