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
def get_date_chunks(date_list):

    n_chunks = 8
    intvl = mf.get_nominal_interval(product)
    limit = intvl * n_chunks
    start_date = date_list[0]
    pairs_list = []
    for date in date_list:
        delta_days = (date - start_date).days
        if delta_days > limit:
            pairs_list.append([start_date, prev_date])
            start_date = date
        prev_date = date
    pairs_list.append([start_date, date])
    return pairs_list
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

product_list = ['MOD09A1', 'MYD09A1', 'MOD11A2', 'MYD11A2', 'MOD13Q1', 
                'MYD13Q1', 'MCD15A2H', 'MOD15A2H', 'MYD15A2H', 'MCD15A3H', 
                'MOD16A2', 'MOD17A2H', 'MYD17A2H', 'MOD17A3H', 'MYD17A3H']
site_df = get_ozflux_site_list(master_file_path)

# Iterate over sites and check dir exists (make if not)
for site in site_df.index:
    print 'Starting processing for site: {}'.format(site)
    site_name = '_'.join(site.split(' '))
    site_dir_path = os.path.join(output_path, site_name)
    check_dir(site_dir_path)
    lat, lon = site_df.loc[site, 'Latitude'], site_df.loc[site, 'Longitude']
    
    # Iterate over products and check dir exists (make if not) and get 
    # available dates
    for product in ['MYD09A1']:#product_list:
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
                if dates_to_retrieve: 
                    date_str = (', ').join(map(lambda x: str(x), 
                                           dates_to_retrieve))
                    print ('The following dates will be appended to product '
                           '{0}, band {1}: {2}'.format(product, band, date_str))
                else:
                    print 'No new data to append... continuing'
                    continue
                date_chunks = get_date_chunks(dates_to_retrieve)
                for chunk in date_chunks:
                    x = mf.modis_data_by_npixel(lat, lon, product, band, 
                                                start_date = chunk[0],
                                                end_date = chunk[-1],
                                                site = site)
                    if x.valid_rows == 0: continue
                    x.write_to_file(product_site_dir_path)
            else:
                x = mf.modis_data_by_npixel(lat, lon, product, band, site = site)
                if x.valid_rows == 0: continue
                x.write_to_file(product_site_dir_path)
#------------------------------------------------------------------------------
