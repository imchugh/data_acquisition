#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 14:38:30 2018

@author: ian
"""
import datetime as dt
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
from suds.client import Client
import webbrowser
import pdb

wsdlurl = ('https://modis.ornl.gov/cgi-bin/MODIS/soapservice/'
           'MODIS_soapservice.wsdl')

class modis_data(object):
    
    #--------------------------------------------------------------------------
    '''
    Object containing MODIS subset data
    
    Args:
        * latitude (int or float): decimal latitude of location
        * longitude (int or float): decimal longitude of location
        * product (str): MODIS product for which to retrieve data (note that 
          not all products are available from the web service - use the 
          'get_product_list()' function of this module for a list of the 
          available products)'
        * band (str): MODIS product band for which to retrieve data (use the 
          'get_band_list(<product>)' function for a list of the available 
          bands)
        * start_date (python datetime): first date for which data is required
        * end_date (python datetime): last date for which data is required
        * subset_height_km (int): distance in kilometres (centred on location)
          from upper to lower boundary of subset
        * subset_width_km (int): distance in kilometres (centred on location)
          from left to right boundary of subset
    
    Returns:
        * MODIS data class containing the following:
            * band (attribute): MODIS band selected for retrieval
            * cellsize (attribute): actual width of pixel in m
            *
    '''
    def __init__(self, latitude, longitude, product, band, 
                 start_date, end_date,
                 subset_height_km = 0, subset_width_km = 0, site = None):
        
        if not product in get_product_list():
            raise KeyError('Product not available from web service! Check '
                           'available products list using get_product_list()')
        if not band in get_band_list(product):
            raise KeyError('Band not available for {}! Check available bands '
                           'list using get_band_list(product)'.format(product))
        self.latitude = latitude
        self.longitude = longitude
        self.product = product
        self.band = band
        self.qc_band = get_qc_variable_band(product, band)
        self.start_date = start_date
        self.end_date = end_date
        self.subset_height_km = subset_height_km
        self.subset_width_km = subset_width_km
        self.site = site
        self.data = self._compile_data(init = True)
    #-------------------------------------------------------------------------- 

    #--------------------------------------------------------------------------
    def _compile_data(self, init = False):

        grouped_dates = self._find_dates()
        if not self.qc_band: bands = [self.band]
        if self.qc_band: bands = [self.band] + [self.qc_band]
        df_list = []
        for this_band in bands:
            data_list = []
            date_list = []
            if this_band == self.band: 
                print ('Fetching primary data from server for dates:')
            else:
                print ('Fetching QC data from server for dates:')
            for date_pair in grouped_dates:
                data = get_subset_data(self.latitude, self.longitude, 
                                       self.product, this_band, 
                                       date_pair[0], date_pair[1], 
                                       self.subset_height_km, 
                                       self.subset_width_km)
                npixels = int(data.ncols * data.nrows)
                for line in data.subset:
                    vals = map(lambda x: int(x), line.split(',')[-npixels:])
                    date = str(line.split(',')[2])
                    date_list.append(date)
                    if this_band == self.band: 
                        if data.scale: vals = map(lambda x: x * data.scale, vals)
                    else:
                        vals = _convert_binary(vals, self.product)
                    data_list.append(vals)
                    print '{},'.format(date[1:]),
            df_index = map(lambda x: dt.datetime.strptime(x, 'A%Y%j').date(),
                           date_list)
            df_cols = map(lambda x: self._make_name(this_band, x), 
                          range(npixels))
            df_list.append(pd.DataFrame(data_list, 
                                        index = df_index, 
                                        columns = df_cols))
            print
        if init:
            self.xllcorner = data.xllcorner
            self.yllcorner = data.yllcorner
            self.cellsize = data.cellsize
            self.nrows = data.nrows
            self.ncols = data.ncols
            self.npixels = npixels
            self.centerpixel = npixels / 2
            self.units = data.units
            self.scale = data.scale
        return pd.concat(df_list, axis = 1)
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def average_pixels(self, pixel_quality = None):
        
        if pixel_quality: 
            try:
                assert pixel_quality in ['High', 'Acceptable']
            except:
                raise KeyError('pixel_quality options are: "High", ' 
                               ' "Acceptable" or None')
        if self.npixels == 1: return
        df = self.data.copy()
        qc_name = get_qc_variable_band(self.product)
        if qc_name and pixel_quality:
            if pixel_quality == 'High': threshold = 0
            if pixel_quality == 'Acceptable': threshold = get_qc_threshold(self.product)
            for pixel in xrange(self.npixels):
                var_str = self._make_name(self.band, pixel)
                qc_str = self._make_name(qc_name, pixel)
                df.loc[df[qc_str] > threshold, var_str] = np.nan
        var_list = filter(lambda x: self.band in x, df.columns)
        return df[var_list].mean(axis = 1)
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def _find_dates(self, n_per_chunk = 8):
        
        modis_date_list = get_date_list(self.latitude, self.longitude, 
                                        self.product)
        start_date = dt.datetime.strftime(self.start_date, 'A%Y%j')
        end_date = dt.datetime.strftime(self.end_date, 'A%Y%j')
        included_dates = filter(lambda x: start_date <= x <= end_date, 
                                modis_date_list)
        if len(included_dates) == 0: raise RuntimeError('No data available '
                                                        'between requested '
                                                        'dates!')     
        date_chunks = map(lambda i: included_dates[i: i + n_per_chunk], 
                          range(0, len(included_dates), n_per_chunk))
        return map(lambda x: (x[0], x[-1]), date_chunks)
    #--------------------------------------------------------------------------
        
    #--------------------------------------------------------------------------
    def get_pixel_by_num(self, pixel_num, mask_by_qc = False):
        
        accept_range = range(int(self.nrows * self.ncols))
        if not pixel_num in accept_range: 
            raise KeyError('pixel_num must be an int between {0} and {1}'
                           .format(int(accept_range[0]), int(accept_range[-1])))
        var_name = self._make_name(self.band, pixel_num)  
        qc_name = self._make_name(get_qc_variable_band(self.product, self.band), 
                                   pixel_num)
        temp_df = self.data[[var_name, qc_name]].copy()
        if mask_by_qc:
            threshold = get_qc_threshold(self.product)
            temp_df.loc[temp_df[qc_name] > threshold, var_name] = np.nan
        return temp_df
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def _make_name(self, band, n):
        
        return '{0}_pixel_{1}'.format(band, str(n).zfill(2))
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def plot_data(self, pixel = None):
        
        if int(filter(lambda x: x.isdigit(), self.product)[:2]) == 12:
            print 'Plotting not implemented for land cover class!'
            return
        fig, ax = plt.subplots(1, 1, figsize = (14, 8))
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)
        ax.tick_params(axis = 'y', labelsize = 14)
        ax.tick_params(axis = 'x', labelsize = 14)
        ax.set_xlabel('Date', fontsize = 14)
        ax.set_ylabel('{0} ({1})'.format(self.band, self.units), fontsize = 14)
        average_series = self.average_pixels(pixel_quality = 'High')
        ax.plot(average_series.index, average_series, 
                label = 'Average ({} pixels)'.format(self.npixels))
        if pixel:
            var_name = '{0}_pixel_{1}'.format(self.band, str(pixel))
            if not self.band:
                ax.plot(self.data.index, self.data[var_name], marker = 'o', 
                        mfc = 'blue', ls = '', alpha = 0.5)
            else:
                qc_name = '{0}_pixel_{1}'.format(self.qc_band, str(pixel))
                threshold = get_qc_threshold(self.product)
                best_df = self.data.loc[self.data[qc_name] == 0]
                ax.plot(best_df.index, best_df[var_name], marker = 'o', 
                        mfc = 'green', ls = '', alpha = 0.5)
                good_df = self.data.loc[(self.data[qc_name] > 0) &
                                        (self.data[qc_name] <= threshold)]
                ax.plot(good_df.index, good_df[var_name], marker = 'o', 
                        mfc = 'orange', ls = '', alpha = 0.5)
                bad_df = self.data.loc[self.data[qc_name] > threshold]
                ax.plot(bad_df.index, bad_df[var_name], marker = 'o', 
                        mfc = 'red', ls = '', alpha = 0.5)
        fig.show()
        return
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def write_to_dir(self, path, append = True):
        assert os.path.isdir(path)
        file_name_list = []
        file_name_list.append(self.site)
        file_name_list.append(self.product)
        file_name_list.append(self.band)
        file_name_str = '{}.csv'.format('_'.join(file_name_list))
        full_path_str = os.path.join(path, file_name_str)
        if not append or not os.path.exists(full_path_str):
            self.data.to_csv(full_path_str, index_label = 'Date')
            return
        else:
            df = pd.read_csv(full_path_str)
            df.index = map(lambda x: dt.datetime.strptime(x, '%Y-%m-%d').date(), 
                           df.Date)
            df.drop('Date', axis = 1, inplace = True)
            df_list = []
            start_date = min([self.data.index[0], df.index[0]])
            end_date = max([self.data.index[-1], df.index[-1]])
            available_dates = map(lambda x: dt.datetime.strptime(x[1:], 
                                                                 '%Y%j').date(),
                                  get_date_list(self.latitude, self.longitude,
                                                self.product))
            included_dates = filter(lambda x: start_date <= x <= end_date, 
                                    available_dates)
            missing_dates = []
            for date in included_dates:
                try:
                    df_list.append(pd.DataFrame(df.loc[date]).transpose())
                except KeyError:
                    try:
                        df_list.append(pd.DataFrame(self.data.loc[date]).transpose())
                    except KeyError:
                        missing_dates.append(date)
            if missing_dates: 
                dates_str = ', '.join(map(lambda x: dt.datetime.strftime(x, 
                                                                         '%Y%j'),
                                          missing_dates))
                print 'Missing data for {}!'.format(dates_str)
            pd.concat(df_list).to_csv(full_path_str, index_label = 'Date')
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _convert_binary(int_list, product):
    bitmap = get_qc_bitmap(product)
    new_list = []
    for this_int in int_list:
        assert isinstance(this_int, int)
        long_bin_val = '{0:b}'.format(int(this_int)).zfill(8)
        trunc_bin_val = int(''.join([long_bin_val[i] for i, x in 
                                     enumerate(bitmap) if x.isdigit()]), 2)
        new_list.append(trunc_bin_val)
    return new_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------    
def get_band_list(product):
    client = Client(wsdlurl)
    return client.service.getbands(product)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_date_list(lat, lon, product):
    
    client = Client(wsdlurl)
    return client.service.getdates(lat, lon, product)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_product_id(product):
    
    return int(filter(lambda x: x.isdigit(), product)[:2])    
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_product_list():
    
    client = Client(wsdlurl)
    return map(lambda x: str(x), client.service.getproducts())
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_pixel_resolution(product):
    resolution_dict = {'09': 500,
                       '11': 1000,
                       '12': 500, 
                       '13': 250,
                       '15': 500,
                       '16': 500,
                       '17': 500}
    id_num = str(_get_product_id(product)).zfill(2)
    return resolution_dict[id_num]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_product_web_page(product = None):
    
    url_dict = {this_product: '{}_v006'.format(this_product.lower()) 
                for this_product in get_product_list()} 
    url_dict['MCD12Q1'] = 'mcd12q1'
    url_dict['MCD12Q2'] = 'mcd12q2'
    if not product:
        addr = ('https://lpdaac.usgs.gov/dataset_discovery/modis/'
                'modis_products_table')
    else:
        addr = ('https://lpdaac.usgs.gov/dataset_discovery/modis/'
                'modis_products_table/{}'.format(url_dict[product]))
    webbrowser.open(addr)
#--------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_qc_bitmap(product):
    
    id_num = _get_product_id(product)
    if id_num <= 13: return 'XXXXXX00'
    return '000XXXXX'
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_qc_definition(product):
    
    idx_dict = {'1': [9, 11],
                '2': [12],
                '3': [13],
                '4': [15, 16],
                '5': [17]}
    
    qc_dict = {'1': {0: ('Pixel produced, good quality, not necessary '
                         'to examine more detailed QA'),
                     1: ('Pixel produced, unreliable or unquantifiable '
                         'quality, recommend examination of more '
                         'detailed QA'),
                     2: 'Pixel not produced due to cloud effects',
                     3: ('Pixel not produced primarily due to reasons '
                         'other than cloud (such as ocean pixel, poor '
                         'input data)')},
               '2': {0: 'processed, good quality',
                     1: 'processed, see other QA',
                     2: 'not processed due to cloud effects',
                     3: 'not processed due to other effects'},
               '3': {0: 'VI produced with good quality',
                     1: 'VI produced, but check other QA',
                     2: 'Pixel produced, but most probably cloudy',
                     3: ('Pixel not produced due to other reasons than '
                                'cloud')},
               '4': {0: ('Main method used, best result possible '
                         '(no saturation)'),
                     1: ('Main method used with saturation. Good, very '
                         'usable'),
                     2: ('Main method failed due to bad geometry, '
                         'empirical algorithm used'),
                     3: ('Main method failed due to problems other '
                         'than geometry, empirical algorithm used'),
                     4: ('Pixel not produced at all, value couldnt '
                         'be retrieved (possible reasons: bad L1B '
                         'data, unusable MOD09GA data)')},
                '5': {0: 'Very best possible',
                      1: 'Good,very usable, but not the best',
                      2: ('Substandard due to geometry problems - '
                          'use with caution'),
                      3: ('Substandard due to other than geometry '
                          'problems - use with caution'),
                      4: ('Couldnt retrieve pixel (NOT PRODUCED AT ALL '
                          '- non-terrestrial biome)'),
                      7: 'Fill Value'}}

    lookup_key = _get_product_id(product)
    idx = filter(lambda x: lookup_key in idx_dict[x], idx_dict.keys())[0]
    return qc_dict[idx]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_qc_threshold(product):
    
    id_num = _get_product_id(product)
    if id_num <= 13: return 1 
    return 3
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_qc_variable_band(product, band = None):
    
    varnames_dict = {'9': 'sur_refl_qc_500m',
                     '11': ['QC_Day', 'QC_Night'],
                     '12': None,
                     '13': '250m_16_days_VI_Quality',
                     '15': 'FparLai_QC',
                     '16': 'ET_QC_500m',
                     '17': 'Psn_QC_500m'}
    id_num = filter(lambda x: x.isdigit(), product)[:2]
    qc_var = varnames_dict[id_num]
    if id_num == '11':
        if not band:
            print ('Product {} has daytime and nighttime qc bands; '
                   'pass band as kwarg to select QC variable(s)'.format(product))
            return None
        elif 'day' in band.lower(): 
            qc_var = filter(lambda x: 'day' in x.lower(), qc_var)
        elif 'night' in band.lower():
            qc_var = filter(lambda x: 'night' in x.lower(), qc_var)
        else:
            return None
    return qc_var
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_subset_data(lat, lon, product, band, start_date, end_date, 
                    subset_height_km = 0, subset_width_km = 0):

    assert start_date <= end_date
    client = Client(wsdlurl)
    return client.service.getsubset(lat, lon, product, band, 
                                    start_date, end_date, 
                                    subset_height_km, subset_width_km)
#------------------------------------------------------------------------------       