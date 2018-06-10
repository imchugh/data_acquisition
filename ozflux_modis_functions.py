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
for site in site_df.index:
    site_name = '_'.join(site.split(' '))
    site_dir_path = os.path.join(output_path, site_name)
    if not os.path.isdir(site_dir_path): os.mkdir(site_dir_path)
    lat = site_df.loc[site, 'Latitude']
    lon = site_df.loc[site, 'Longitude']
    for product in product_list:
        band_list = mf.get_band_list(product)
        if product in drop_bands_dict.keys():
            for band in drop_bands_dict[product]:
                try: band_list.remove(band)
                except ValueError: pass
        if '11' in product:
            for band in ['day', 'night']:
                qc_band = mf.get_qc_variable_band(product, band)
                band_list.remove(qc_band)
        else:
            qc_band = mf.get_qc_variable_band(product)
            if qc_band: band_list.remove(qc_band)
        product_site_dir_path = os.path.join(site_dir_path, product)
        product_dates_available = map(lambda x: 
                                      dt.datetime.strptime(x[1:], '%Y%j').date(),
                                      mf.get_date_list(lat, lon, product))
        if not os.path.isdir(product_site_dir_path): 
            os.mkdir(product_site_dir_path)        
        for band in band_list:
            try:
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
            except:
                continue
#------------------------------------------------------------------------------