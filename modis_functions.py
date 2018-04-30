#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 14:38:30 2018

@author: ian
"""
from suds.client import Client

import datetime as dt
import matplotlib.pyplot as plt
import pandas as pd
import webbrowser
import xlrd

wsdlurl = ('https://modis.ornl.gov/cgi-bin/MODIS/soapservice/'
           'MODIS_soapservice.wsdl')

class modis_data(object):
    
    #--------------------------------------------------------------------------
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
        self.start_date = start_date
        self.end_date = end_date
        self.subset_height_km = subset_height_km
        self.subset_width_km = subset_width_km
        self.site = site
        self.data = self.compile_data()
    #-------------------------------------------------------------------------- 

    #--------------------------------------------------------------------------
    def compile_data(self):
        
        modis_date_list = get_date_list(self.latitude, self.longitude, 
                                        self.product)
        refmt_dates = map(lambda x: dt.datetime.strftime(x, 'A%Y%j'), 
                          [self.start_date, self.end_date])
        included_dates = filter(lambda x: refmt_dates[0] <= x <= refmt_dates[1], 
                                modis_date_list)
        chunked_dates = _chunk_dates(included_dates)
        data_dict, date_list = {}, []
        qc_band = get_qc_variable_band(self.product, self.band)
        if qc_band[0] is None: 
            bands = [self.band]
        else:
            bands = [self.band] + qc_band
        for i, this_band in enumerate(bands):
            if i == 0: print 'Fetching primary data from server for dates:'
            if i == 1: print 'Fetching QC data from server for dates:'
            data_list = []
            for date_pair in chunked_dates:
                data = get_subset_data(self.latitude, self.longitude, 
                                       self.product, this_band, 
                                       date_pair[0], date_pair[1], 
                                       self.subset_height_km, 
                                       self.subset_width_km)
                for line in data.subset:
                    data_list.append(int(line.split(',')[-1]))
                    date = str(line.split(',')[2])
                    print '{},'.format(date[1:]),
                    if i == 0: date_list.append(date)
            print                      
            if i == 0:
                if not data.scale == 0: 
                    data_dict[this_band] = map(lambda x: x * data.scale, 
                                               data_list)
                else:
                    data_dict[this_band] = data_list
                date_list = map(lambda x: dt.datetime.strptime(x, 'A%Y%j').date(),
                                date_list)
                self.xllcorner = data.xllcorner
                self.yllcorner = data.yllcorner
                self.cellsize = data.cellsize
                self.nrows = data.nrows
                self.ncols = data.ncols
                self.units = data.units
                self.scale = data.scale
            else:
                data_dict[this_band] = _convert_binary(data_list, self.product)
        return pd.DataFrame(data_dict, index = date_list)
    #--------------------------------------------------------------------------
    def plot_data(self):
        
        qc_band = ','.join(get_qc_variable_band(self.product, self.band))
        threshold = get_qc_threshold(self.product)
        fig, ax = plt.subplots(1, 1, figsize = (14, 8))
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)
        ax.tick_params(axis = 'y', labelsize = 14)
        ax.tick_params(axis = 'x', labelsize = 14)
        ax.set_xlabel('Date', fontsize = 14)
        ax.set_ylabel('{0} ({1})'.format(self.band, self.units), fontsize = 14)
        best_df = self.data.loc[self.data[qc_band] == 0]
        ax.plot(best_df.index, best_df[self.band], marker = 'o', mfc = 'green',
                ls = '', alpha = 0.5)
        good_df = self.data.loc[(self.data[qc_band] > 0) &
                                (self.data[qc_band] <= threshold)]
        ax.plot(good_df.index, good_df[self.band], marker = 'o', mfc = 'orange',
                ls = '', alpha = 0.5)
        bad_df = self.data.loc[self.data[qc_band] > threshold]
        ax.plot(bad_df.index, bad_df[self.band], marker = 'o', mfc = 'red',
                ls = '', alpha = 0.5)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _chunk_dates(date_list, n_per_chunk = 8):
    
    date_chunks = map(lambda i: date_list[i: i + n_per_chunk], 
                      range(0, len(date_list), n_per_chunk))
    return map(lambda x: (x[0], x[-1]), date_chunks)    
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
def get_product_list():
    client = Client(wsdlurl)
    return map(lambda x: str(x), client.service.getproducts())
#------------------------------------------------------------------------------

#--------------------------------------------------------------------------
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
    
    id_num = int(filter(lambda x: x.isdigit(), product)[:2])
    if id_num <= 13: 
        return 'XXXXXX00'
    else:
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
                     4: ('Pixel not produced at all, value couldn’t '
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

    lookup_key = int(filter(lambda x: x.isdigit(), product)[:2])
    idx = filter(lambda x: lookup_key in idx_dict[x], idx_dict.keys())[0]
    return qc_dict[idx]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_qc_threshold(product):
    
    id_num = int(filter(lambda x: x.isdigit(), product)[:2])
    threshold = 1 if id_num <= 13 else 3 
    return threshold
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
    if not id_num == '11':
        qc_var = [qc_var]
    else:
        if not band:
            print 'Pass band as kwarg to select QC variable(s)'
        elif 'day' in band.lower(): 
            qc_var = filter(lambda x: 'day' in x.lower(), qc_var)
        elif 'night' in band.lower(): 
            qc_var = filter(lambda x: 'night' in x.lower(), qc_var)
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
def get_ozflux_modis_data(site, product, band, start_date, end_date,
                          subset_height_km = 0, subset_width_km = 0):
    
    site_df = get_ozflux_site_list()
    latitude = site_df.loc[site, 'Latitude']
    longitude = site_df.loc[site, 'Longitude']
    return modis_data(latitude, longitude, product, band, start_date, end_date,
                      subset_height_km, subset_width_km, site)
#------------------------------------------------------------------------------