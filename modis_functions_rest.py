#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 14:38:30 2018

@author: ian
"""
from collections import OrderedDict
import datetime as dt
import json
import math
import matplotlib.pyplot as plt
import numpy as np
import os
import pandas as pd
import requests
import xarray as xr




import pdb

wsdlurl = ('https://modis.ornl.gov/cgi-bin/MODIS/soapservice/'
           'MODIS_soapservice.wsdl')

api_base_url = 'https://modis.ornl.gov/rst/api/v1/'

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
def get_band_list(product, include_details = True):
    
    json_obj = requests.get(api_base_url + product + '/bands')
    band_list = json.loads(json_obj.content)['bands']
    d = OrderedDict(zip([x.pop('band') for x in band_list], band_list))
    if include_details: return d
    return d.keys()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_chunks(l, n = 10):

    """yield successive n-sized chunks from list l"""
    
    for i in range(0, len(l), n): yield l[i: i + n]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_product_dates(product, lat, lng):
    
    req_str = "".join([api_base_url, product, "/dates?", "latitude=", str(lat), 
                       "&longitude=", str(lng)])
    json_obj = requests.get(req_str)
    date_list = json.loads(json_obj.content)['dates']
    return date_list
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_product_id(product):
    
    return int(filter(lambda x: x.isdigit(), product)[:2])    
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_product_list(include_details = True):
    
    """Get list of available products"""
    
    json_obj = requests.get(api_base_url + 'products')
    products_list = json.loads(json_obj.content)['products']
    d = OrderedDict(zip([x.pop('product') for x in products_list], 
                        products_list))
    if include_details: return d
    return d.keys()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_network_list(network = None):
    
    """Get list of available networks (if None) or sites within network if 
       network supplied"""
    
    if network == None: 
        json_obj = requests.get(api_base_url + 'networks')
        return json.loads(json_obj.content)['networks']
    rq_url = api_base_url + '{}/sites'.format(network)
    json_obj = requests.get(rq_url)
    sites_list = json.loads(json_obj.content)
    return OrderedDict(zip([x.pop('network_sitename') 
                            for x in sites_list['sites']], sites_list['sites']))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _getSubsetURL(prod, lat, lng, band, date_start, date_end, ab, lr): 

    """assemble request URL string"""

    return( "".join([api_base_url, prod, "/subset?",
                     "latitude=", str(lat),
                     "&longitude=", str(lng),
                     "&band=", str(band),
                     "&startDate=", str(date_start),
                     "&endDate=", str(date_end),
                     "&kmAboveBelow=", str(ab),
                     "&kmLeftRight=", str(lr)]))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _process_data(data):
    
    """Process the raw data into a more human-intelligible format (xarray)"""
    
    meta = { key:value for key,value in data[0].items() if key != "subset" }
    data_dict = {'dates': [], 'arrays': [], 'metadata': meta}
    for i in data:
        for j in i['subset']:
            data_dict['dates'].append(j['calendar_date'])
            data_dict['arrays'].append(np.array(j['data']).reshape(meta['nrows'], 
                                                                   meta['ncols']))        
    dtdates = [ dt.datetime.strptime(d,"%Y-%m-%d") for d in data_dict['dates']]
    xcoordinates = ([float(meta['xllcorner'])] + 
                    [i * meta['cellsize'] + float(meta['xllcorner']) 
                     for i in range(1, meta['ncols'])])
    ycoordinates = ([float(meta['yllcorner'])] + 
                     [i * meta['cellsize'] + float(meta['yllcorner'])
                      for i in range(1, meta['nrows'])])
    return xr.DataArray(name = meta['band'],
                        data = np.flipud(np.dstack(data_dict['arrays'])),
                        coords = [np.array(ycoordinates), 
                                  np.array(xcoordinates), dtdates],
                        dims = [ "y", "x", "time" ],
                        attrs = meta)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def request_subset(prod, lat, lng, band, date_start, date_end, ab = 0, lr = 0):
    
    """Get the data from ORNL DAAC"""
    
    header = {'Accept': 'application/json'}
    dates = [x['modis_date'] for x in get_product_dates(prod, lat, lng)]
    pydates_arr = np.array([modis_to_from_pydatetime(x) for x in dates])
    start_idx = abs(pydates_arr - date_start).argmin()
    end_idx = abs(pydates_arr - date_end).argmin()
    date_chunks = list(get_chunks(dates[start_idx: end_idx]))
    subsets = []
    for i, chunk in enumerate(date_chunks):
        print '[{} / {}]'.format(str(i + 1), str(len(date_chunks)))
        url = _getSubsetURL(prod, lat, lng, band, chunk[0], chunk[-1], ab, lr)
        response = requests.get(url , headers = header)
        subsets.append(json.loads(response.text))
    return _process_data(subsets)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def modis_to_from_pydatetime(date):
    
    if isinstance(date, (str, unicode)): 
        return dt.datetime.strptime(date[1:], '%Y%j').date()
    return dt.datetime.strftime(date, 'A%Y%j')
#------------------------------------------------------------------------------