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
class ozflux_data_generator(object):

    #--------------------------------------------------------------------------    
    '''
    '''
    def __init__(self, master_file_path):
        self.site_list = get_ozflux_site_list(master_file_path)
        self._subset_class = mf.modis_data
    #--------------------------------------------------------------------------        
    
    #--------------------------------------------------------------------------
    def ozflux_modis_data(self, site, product, band, start_date, end_date,
                          pixels_per_side):
        
        try:
            assert isinstance(pixels_per_side, int)
            assert pixels_per_side % 2 != 0
        except AssertionError:
            raise TypeError('Arg "pixels_per_side" must be an odd-numbered '
                            'integer')
        lat = self.site_list.loc[site, 'Latitude']
        lon = self.site_list.loc[site, 'Longitude']
        resolution_km = _get_length_from_pixel_n(product, pixels_per_side)
        data_class = self._subset_class(lat, lon, product, band, 
                                        start_date, end_date,
                                        resolution_km, resolution_km, site)
        n_pixels_required = pixels_per_side ** 2
        center_pixel = data_class.npixels / 2
        start_pixel_n = center_pixel - n_pixels_required / 2
        end_pixel_n = center_pixel + n_pixels_required / 2
        data_names = sorted(filter(lambda x: data_class.band in x, 
                                   data_class.data.columns))
        new_data_names = map(lambda x: data_class._make_name(data_class.band, x),
                             range(n_pixels_required))
        subset_data_names = data_names[start_pixel_n: end_pixel_n + 1]
        if data_class.qc_band:
            qc_names = sorted(filter(lambda x: data_class.qc_band in x, 
                                     data_class.data.columns))
            new_qc_names = map(lambda x: data_class._make_name(data_class.qc_band, x),
                               range(n_pixels_required))
            subset_qc_names = qc_names[start_pixel_n: end_pixel_n + 1]
            subset_data_names = subset_data_names + subset_qc_names 
            new_data_names = new_data_names + new_qc_names
        new_df = data_class.data[subset_data_names].copy()
        new_df.columns = new_data_names
        data_class.data = new_df
        
        return data_class
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
def _get_length_from_pixel_n(product, pixels_per_side):
    
    pixel_res = mf.get_pixel_resolution(product)
    center_edge_min = math.ceil((pixels_per_side - 2) * pixel_res / 2)
    return int(math.ceil(center_edge_min / 1000))
#------------------------------------------------------------------------------

def _subset_dataframe(data_class, n_pixels_required):
    
    pass
        
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