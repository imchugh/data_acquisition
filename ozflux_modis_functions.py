#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Wed May  9 14:06:55 2018

@author: ian
"""

import math
import numpy as np
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
def write_site_data(master_file_path, output_path):
    
    site_df = get_ozflux_site_list(master_file_path)
    for site in site_df.index:
        lat = site_df.loc[site, 'Latitude']
        lon = site_df.loc[site, 'Longitude']
        for product in retrieval_dict.keys():
            for band in retrieval_dict[product]:
                x = mf.modis_data_by_npixel(lat, lon, product, band, site = site)
                x.write_to_dir(output_path)
    pdb.set_trace()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
retrieval_dict = {'MOD13Q1': ['250m_16_days_NDVI', '250m_16_days_EVI']}