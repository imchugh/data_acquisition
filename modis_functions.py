#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 14:38:30 2018

@author: ian
"""
import datetime as dt
import math
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
from suds.client import Client
from time import sleep
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
        * start_date (python datetime or None): first date for which data is 
          required, or if None, first date available on server
        * end_date (python datetime): last date for which data is required,
          or if None, last date available on server
        * subset_height_km (int): distance in kilometres (centred on location)
          from upper to lower boundary of subset
        * subset_width_km (int): distance in kilometres (centred on location)
          from left to right boundary of subset
    
    Returns:
        * MODIS data class containing the following:
            * band (attribute): MODIS band selected for retrieval
            * cellsize (attribute): actual width of pixel in m
            * centerpixel (attribute): number of pixel containing 
              user-specified coordinates
            * data (attribute): pandas dataframe containing primary and qc data 
              (where applicable) for user-specified product and band
            * 
    '''
    def __init__(self, latitude, longitude, product, band, 
                 start_date = None, end_date = None,
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
        self.chunk_size = 8
        self.qc_band = get_qc_variable_band(product, band)
        self.qc_is_primary = self._check_if_qc_primary()
        self.start_date = start_date
        self.end_date = end_date
        self.subset_height_km = subset_height_km
        self.subset_width_km = subset_width_km
        self.site = site if site else 'Unknown'
        self.data = self._compile_data()
    #-------------------------------------------------------------------------- 

    #--------------------------------------------------------------------------
    def _check_if_qc_primary(self):
        
        if self.band == self.qc_band or 'QC' in self.band: 
            self.qc_band = None
            return True
        return False
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _check_line_integrity(self, subset, band):

        data_list = []
        date_list = []
        npixels = int(subset.ncols * subset.nrows)
        for line in subset.subset:
            if len(line) == 0: 
                print 'Missing data line... skipping,',
                continue
            line_list = line.split(',')
            if len(line_list) != npixels + 5:
                print 'Some data missing or mangled for date'
                continue
            date = str(line_list[2])
            date_list.append(date)
            vals = map(lambda x: int(x), line_list[-npixels:])
            if not self.qc_is_primary:
                if band == self.band:
                    if subset.scale: 
                        vals = map(lambda x: round(x * subset.scale, 4), 
                                   vals)
                elif band == self.qc_band:
                    vals = _convert_binary(vals, self.product)
            data_list.append(vals)
            print '{},'.format(date[1:]),
        return data_list, date_list
    #--------------------------------------------------------------------------

    #--------------------------------------------------------------------------
    def _compile_data(self):          
                    
        grouped_dates = self._find_dates()
        bands = filter(lambda x: not x is None, [self.band, self.qc_band])
        df_list = []
        print ('Retrieving data for {0} site (product: {1}, band: {2}):'
               .format(self.site, self.product, self.band))
        for this_band in bands:
            data_list = []
            date_list = []
            if this_band == self.band:
                print ('Fetching primary data from server for dates:')
            elif this_band == self.qc_band:
                print ('Fetching QC data from server for dates:')
            for date_pair in grouped_dates:
                try:
                    data = get_subset_data(self.latitude, self.longitude, 
                                           self.product, this_band, 
                                           date_pair[0], date_pair[1], 
                                           self.subset_height_km, 
                                           self.subset_width_km)
                    npixels = int(data.ncols * data.nrows)
                except RuntimeError, e:
                    print e
                    continue
                lines, dates = self._check_line_integrity(data, this_band)
                data_list += lines; date_list += dates
            df_index = map(lambda x: dt.datetime.strptime(x, 'A%Y%j').date(),
                           date_list)
            df_cols = map(lambda x: self._make_name(this_band, x), 
                          range(npixels))
            df_list.append(pd.DataFrame(data_list, 
                                        index = df_index, 
                                        columns = df_cols))
            print
        df = pd.concat(df_list, axis = 1)
        df = df[self._reorder_columns(df.columns)] 
        self.xllcorner = data.xllcorner
        self.yllcorner = data.yllcorner
        self.cellsize = data.cellsize
        self.nrows = data.nrows
        self.ncols = data.ncols
        self.npixels = npixels
        self.centerpixel = npixels / 2
        self.units = data.units
        self.scale = data.scale
        self.valid_rows = len(df)
        if self.valid_rows == 0:
            print 'Warning: server did not return valid data for this run!'
        return df
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def do_sample_stats(self, pixel_quality = None):
        
        if self.npixels == 1: print 'Cannot analyse single pixel'; return
        if len(self.data) == 0: print 'Cannot do stats on empty dataframe!'; return
        if not self.qc_band:
            print ('No qc variable available for this modis product; '
                   'returning unfiltered product')
        df = pd.concat(map(lambda x: self.get_pixel_by_num(x, pixel_quality), 
                           range(self.npixels)), axis = 1)
        stats_df = df.transpose().describe().transpose()
        stats_df['median'] = df.median(axis = 1)
        return stats_df
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def _find_dates(self):
        
        modis_date_list = get_date_list(self.latitude, self.longitude, 
                                        self.product)
        if not self.start_date: 
            start_date = modis_date_list[0]
        else:
            start_date = dt.datetime.strftime(self.start_date, 'A%Y%j')
        if not self.end_date:
            end_date = modis_date_list[-1]
        else:
            end_date = dt.datetime.strftime(self.end_date, 'A%Y%j')
        included_dates = filter(lambda x: start_date <= x <= end_date, 
                                modis_date_list)
        if len(included_dates) == 0: raise RuntimeError('No data available '
                                                        'between requested '
                                                        'dates!')     
        date_chunks = map(lambda i: included_dates[i: i + self.chunk_size], 
                          range(0, len(included_dates), self.chunk_size))
        return map(lambda x: (x[0], x[-1]), date_chunks)
    #--------------------------------------------------------------------------
        
    #--------------------------------------------------------------------------
    def get_pixel_by_num(self, pixel_num, pixel_quality = None):
        
        accept_range = range(self.npixels)
        if not pixel_num in accept_range: 
            raise KeyError('pixel_num must be an int between {0} and {1}'
                           .format(int(accept_range[0]), int(accept_range[-1])))
        if pixel_quality: 
            try:
                assert pixel_quality in ['High', 'Acceptable']
            except:
                raise KeyError('pixel_quality options are: "High", ' 
                               ' "Acceptable" or None')
        var_name = self._make_name(self.band, pixel_num)
        if not pixel_quality: return self.data[var_name].copy()
        if not self.qc_band: 
            return self.data[var_name].copy()
        else:
            qc_name = self._make_name(self.qc_band, pixel_num)
            if pixel_quality == 'High': threshold = 0
            if pixel_quality == 'Acceptable': 
                threshold = get_qc_threshold(self.product)
            df = self.data.copy()
            df.loc[df[qc_name] > threshold, var_name] = np.nan
            return df[var_name]
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
        average_series = self.do_sample_stats(pixel_quality = 'Acceptable')['mean']
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
    def _reorder_columns(self, col_names):
        if not self.qc_band: return col_names
        col_tuples = zip(sorted(filter(lambda x: self.band in x, col_names)),
                         sorted(filter(lambda x: self.qc_band in x, col_names)))
        new_order = [val for sublist in col_tuples for val in sublist]
        return new_order
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------
    def write_to_file(self, path_to_dir, pre_header = None):
        
        try:
            assert os.path.isdir(path_to_dir)
        except AssertionError:
            raise IOError('Specified path does not exist')
        site_name = '_'.join(self.site.split(' '))
        file_name_str = '{0}_{1}_{2}.csv'.format(site_name, self.product, self.band)
        target_file_path = os.path.join(path_to_dir, file_name_str)
        df = self.data.copy().join(self.do_sample_stats(pixel_quality = 
                                                        'Acceptable'))
        if not os.path.isfile(target_file_path):
            self._write_to_new_file(target_file_path, df, pre_header)
        else:
            if pre_header: print 'File exists - ignoring passed pre-header'
            self._write_to_existing_file(target_file_path, df)
    #--------------------------------------------------------------------------    

    #--------------------------------------------------------------------------
    def _write_to_new_file(self, target_file_path, df, pre_header = None):

        if pre_header: 
            if isinstance(pre_header, list):
                if filter(lambda x: not isinstance(x, str), pre_header):
                    raise TypeError('All items in pre_header must be of type str')
                pre_header = '\n'.join(pre_header) + '\n'
            else:
                raise TypeError('pre_header must be of type list')
        if pre_header:
            with open(target_file_path, 'w') as f:
                for line in pre_header:
                    f.write(line)
                df.to_csv(f, index_label = 'Date')
        else:
            df.to_csv(target_file_path, index_label = 'Date')
        return
    #--------------------------------------------------------------------------
        
    #--------------------------------------------------------------------------
    def _write_to_existing_file(self, target_file_path, df):
        
        with open(target_file_path) as f:
            header_list = []
            for i, line in enumerate(f):
                if line.split(',')[0] == 'Date': break
                header_list.append(line)
        old_df = pd.read_csv(target_file_path, skiprows = range(i))
        old_df.index = map(lambda x: dt.datetime.strptime(x, '%Y-%m-%d').date(), 
                           old_df.Date)
        new_df = df.copy()
        new_df['Date'] = map(lambda x: dt.datetime.strftime(x, '%Y-%m-%d'), 
                             new_df.index)
        new_df = new_df[old_df.columns]
        all_df = pd.concat([old_df, new_df])
        all_df.sort_index(inplace = True)
        all_df.drop_duplicates(inplace = True)
        all_df.drop('Date', axis = 1, inplace = True)
        all_df = all_df.loc[~all_df.index.duplicated(keep = 'last')]
        with open(target_file_path, 'w') as f:
            for line in header_list:
                f.write(line)
            all_df.to_csv(f, index_label = 'Date')
    #--------------------------------------------------------------------------
    
#------------------------------------------------------------------------------
class modis_data_by_npixel(modis_data):
    '''
    Inherits from modis_data class above (see docstring), except:
        * subset_width_km and subset_height_km args are omitted, and instead a 
          single argument ("pixels_per_side") is passed, and the appropriate pixel
          matrix is returned with the same attributes and methods as parent class -
          note that the pixels_per_side argument must be an odd-numbered integer 
          so that the pixel containing the passed coordinates is the center pixel
    '''    
    def __init__(self, latitude, longitude, product, band, start_date = None, 
                 end_date = None, pixels_per_side = 3, site = None):
        
        try:
            assert isinstance(pixels_per_side, int)
            assert pixels_per_side % 2 != 0
        except AssertionError:
            raise TypeError('Arg "pixels_per_side" must be an odd-numbered '
                            'integer')
        resolution_km = self._get_length_from_pixel_n(product, pixels_per_side)
        modis_data.__init__(self, latitude, longitude, product, band, 
                            start_date, end_date, resolution_km,
                            resolution_km, site)
        self.data = self._subset_dataframe(pixels_per_side ** 2)
        self.npixels = pixels_per_side ** 2
        self.nrows = pixels_per_side
        self.ncols = pixels_per_side
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------    
    def _get_length_from_pixel_n(self, product, pixels_per_side):
    
        pixel_res = get_pixel_resolution(product)
        center_edge_min = math.ceil((pixels_per_side - 2) * pixel_res / 2)
        return int(math.ceil(center_edge_min / 1000))
    #--------------------------------------------------------------------------
    
    #--------------------------------------------------------------------------    
    def _subset_dataframe(self, n_pixels_required):
    
        center_pixel = self.npixels / 2
        start_pixel_n = center_pixel - n_pixels_required / 2
        end_pixel_n = center_pixel + n_pixels_required / 2
        bands = filter(lambda x: not x is None, [self.band, self.qc_band])
        names_list = []
        new_names_list = []
        for band in bands:
            names = sorted(filter(lambda x: band in x, self.data.columns))
            names = names[start_pixel_n: end_pixel_n + 1]
            names_list = names_list + names
            new_names = map(lambda x: self._make_name(band, x),
                            range(n_pixels_required))
            new_names_list = new_names_list + new_names
        new_df = self.data[names_list].copy()
        new_df.columns = new_names_list
        new_df = new_df[self._reorder_columns(new_df.columns)]
        return new_df
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
def get_nominal_interval(product):
    
    dates_arr = np.array(map(lambda x: dt.datetime.strptime(x[1:], '%Y%j'), 
                         get_date_list(0, 0, product)))
    arr = dates_arr - np.roll(dates_arr, 1)
    unique, counts = np.unique(arr, return_counts = True)
    idx = np.where(counts == counts.max())
    interval_days = unique[idx].item().days
    if counts.max() / float(len(dates_arr)) > 0.8:
        return interval_days
    raise RuntimeError('Could not reliably ascertain interval in days!')
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
    
    products_list = get_product_list()
    modis_url_dict = {prod: '{}_v006'.format(prod.lower()) 
                      for prod in products_list if prod[0] == 'M'}
    modis_url_dict['MCD12Q1'] = 'mcd12q1'
    modis_url_dict['MCD12Q2'] = 'mcd12q2'
    viirs_url_dict = {prod: '{}_v001'.format(prod.lower()) 
                      for prod in products_list if prod[:3] == 'VNP'}
    modis_url_dict.update(viirs_url_dict)
    base_addr = ('https://lpdaac.usgs.gov/dataset_discovery/{0}/'
                 '{0}_products_table')
    if product is None or not product in products_list:
        print 'Product not found... redirecting to data discovery page'
        addr = ('https://lpdaac.usgs.gov/dataset_discovery')
    else:
        fill_str = 'modis' if product[0] == 'M' else 'viirs'
        addr = os.path.join(base_addr.format(fill_str), modis_url_dict[product])
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
    
    varnames_dict = {'MCD12Q1': None,
                     'MCD12Q2': None,
                     'MCD15A2H': 'FparLai_QC',
                     'MCD15A3H': 'FparLai_QC',
                     'MOD09A1': 'sur_refl_qc_500m',
                     'MOD11A2': ['QC_Day', 'QC_Night'],
                     'MOD13Q1': '250m_16_days_VI_Quality',
                     'MOD15A2H': 'FparLai_QC',
                     'MOD16A2': 'ET_QC_500m',
                     'MOD17A2H': 'Psn_QC_500m',
                     'MOD17A3H': 'Npp_QC_500m',
                     'MYD09A1': 'sur_refl_qc_500m',
                     'MYD11A2': ['QC_Day', 'QC_Night'],
                     'MYD13Q1': '250m_16_days_VI_Quality',
                     'MYD15A2H': 'FparLai_QC',
                     'MYD17A2H': 'Psn_QC_500m',
                     'MYD17A3H': 'Npp_QC_500m'}
    
    qc_var = varnames_dict[product]
    if isinstance(qc_var, list):
        if not band:
            print ('Product {} has daytime and nighttime qc bands; '
                   'pass band as kwarg to select QC variable(s)'.format(product))
            return None
        elif 'day' in band.lower():
            qc_var = filter(lambda x: 'day' in x.lower(), qc_var)[0]
        elif 'night' in band.lower():
            qc_var = filter(lambda x: 'night' in x.lower(), qc_var)[0]
        else:
            return None
    return qc_var
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_subset_data(lat, lon, product, band, start_date, end_date, 
                    subset_height_km = 0, subset_width_km = 0):

    assert start_date <= end_date
    client = Client(wsdlurl)
    counter = 0
    exception_list = []
    while True:
        try:
            data = client.service.getsubset(lat, lon, product, band, 
                                            start_date, end_date, 
                                            subset_height_km, subset_width_km)
        except Exception, e:
            counter += 1
            sleep(2)
            if not e in exception_list: 
                if isinstance(e, str): 
                    exception_list.append(e)
                else:
                    exception_list.append('URLError - no standard error string')
            if counter > 10:
                error_str = '{}\n'.format(', '.join(exception_list))
                raise RuntimeError('Server failed to respond 10 times, '
                                   'with following errors : {}. Giving up'
                                   .format(error_str))
            continue
        break
    return data
#------------------------------------------------------------------------------
