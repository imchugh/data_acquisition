#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 16 14:38:30 2018

@author: ian
"""
from collections import OrderedDict
import datetime as dt
import json
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import requests
from scipy import interpolate, signal
import webbrowser
import xarray as xr

import pdb

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
    '''

    def __init__(self, product, band, latitude, longitude, 
                 start_date = None, end_date = None,
                 subset_height_km = 0, subset_width_km = 0, site = None):
        
        if not product in get_product_list(include_details = False):
            raise KeyError('Product not available from web service! Check '
                           'available products list using get_product_list()')
        if not band in get_band_list(product, include_details = False):
            raise KeyError('Band not available for {}! Check available bands '
                           'list using get_band_list(product)'.format(product))
        
        self.site = site
        self.data_array = request_subset_by_coords(product, latitude, 
                                                   longitude, band, start_date,
                                                   end_date, subset_height_km,
                                                   subset_width_km)
    #-------------------------------------------------------------------------- 

class modis_data_network(object):
    
    #--------------------------------------------------------------------------
    '''
    Object containing MODIS subset data
    
    Args:
        * product (str): MODIS product for which to retrieve data (note that 
          not all products are available from the web service - use the 
          'get_product_list()' function of this module for a list of the 
          available products)'
        * band (str): MODIS product band for which to retrieve data (use the 
          'get_band_list(<product>)' function for a list of the available 
          bands)
        * network_name (str): network for which to retrieve data (use the 
          'get_network_list()' function for a list of the available networks)
        * site_ID (str): network site for which to retrieve data (use the 
          'get_network_list(<network>)' function for a list of the available 
          sites and corresponding codes within a network)
        * start_date (python datetime or None): first date for which data is 
          required, or if None, first date available on server
        * end_date (python datetime): last date for which data is required,
          or if None, last date available on server
        * filtered (boolean): whether or not to impose QC filtering on the data
    
    Returns:
        * MODIS data class containing the following:
            * band (attribute): MODIS band selected for retrieval
            * cellsize (attribute): actual width of pixel in m
    '''
    def __init__(self, product, band, network_name, site_ID,  
                 start_date = None, end_date = None, filtered = False):

        if not product in get_product_list(include_details = False):
            raise KeyError('Product not available from web service! Check '
                           'available products list using get_product_list()')
        if not band in get_band_list(product):
            raise KeyError('Band not available for {}! Check available bands '
                           'list using get_band_list(product)'.format(product))
        if not network_name in get_network_list():
            raise KeyError('Network not available from web service! Check '
                           'available networks list using get_network_list()')
        if not site_ID in get_network_list(network_name, include_details = False):
            raise KeyError('Site ID code not found! Check available site ID '
                           'codes using get_network_list(network)'.format(product))
        
        self.site_attrs = get_network_list(network_name)[site_ID]
        if start_date is None or end_date is None:
            dates = get_product_dates(product, 
                                      self.site_attrs['latitude'],
                                      self.site_attrs['longitude'])
        if start_date is None: 
            start_date = modis_to_from_pydatetime(dates[0]['modis_date'])
        if end_date is None: 
            end_date = modis_to_from_pydatetime(dates[-1]['modis_date'])
        self.data_array = request_subset_by_siteid(product, band, network_name,
                                                   site_ID, start_date, end_date, 
                                                   filtered = filtered)
    #--------------------------------------------------------------------------
    def plot_data(self, pixel_num = 'center', smooth = True):
        
        data = get_pixel_subset(self.data_array, 3)
        df = interp_and_filter(data)
        fig, ax = plt.subplots(1, 1, figsize = (14, 8))
        ax.spines['right'].set_visible(False)
        ax.spines['top'].set_visible(False)
        ax.tick_params(axis = 'y', labelsize = 14)
        ax.tick_params(axis = 'x', labelsize = 14)
        ax.set_xlabel('Date', fontsize = 14)
        ax.set_ylabel(self.data_array.attrs['band'], fontsize = 14)
        ax.plot(df.index, df.data_interp, lw = 0.5)
        ax.plot(df.index, df.data_smooth, lw = 2)
    #--------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _error_codes(json_obj):
    
    d = {400: 'Invalid band for product',
         404: 'Product not found'}
    
    status_code = json_obj.status_code
    if status_code == 200: return
    try: 
        error = d[status_code]
    except KeyError:
        error = 'Unknown error code ({})'.format(str(status_code))
    raise RuntimeError('retrieval failed - {}'.format(error))
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
def _get_chunks(l, n = 10):

    """yield successive n-sized chunks from list l"""
    
    for i in range(0, len(l), n): yield l[i: i + n]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def _get_default_dates(product, band, start = True):
    
    return
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
def get_product_web_page(product = None):
    
    products_list = get_product_list()
    modis_url_dict = {prod: '{}v006'.format(prod.lower()) for prod in 
                      products_list if prod[0] == 'M'}
    viirs_url_dict = {prod: '{}v001'.format(prod.lower()) 
                      for prod in products_list if prod[:3] == 'VNP'}
    modis_url_dict.update(viirs_url_dict)
    base_addr = ('https://lpdaac.usgs.gov/products/{0}')
    if product is None or not product in modis_url_dict.keys():
        print 'Product not found... redirecting to data discovery page'
        addr = ('https://lpdaac.usgs.gov')
    else:
        addr = base_addr.format(modis_url_dict[product])
    webbrowser.open(addr)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_network_list(network = None, include_details = True):
    
    """Get list of available networks (if None) or sites within network if 
       network name supplied"""
    
    if network == None: 
        json_obj = requests.get(api_base_url + 'networks')
        return json.loads(json_obj.content)['networks']
    rq_url = api_base_url + '{}/sites'.format(network)
    json_obj = requests.get(rq_url)
    sites_list = json.loads(json_obj.content)
    d = OrderedDict(zip([x.pop('network_siteid') for x in sites_list['sites']], 
                    sites_list['sites']))
    if include_details: return d
    return d.keys()
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def modis_to_from_pydatetime(date):
    
    """Convert between MODIS date strings and pydate format"""
    
    if isinstance(date, (str, unicode)): 
        return dt.datetime.strptime(date[1:], '%Y%j').date()
    return dt.datetime.strftime(date, 'A%Y%j')
#------------------------------------------------------------------------------
    
#------------------------------------------------------------------------------
def _process_data(data):
    
    """Process the raw data into a more human-intelligible format (xarray)"""

    try:
        meta = {key:value for key,value in data[0].items() if key != "subset" }
    except AttributeError:
        pdb.set_trace()
    data_dict = {'dates': [], 'arrays': [], 'metadata': meta}
    for i in data:
        for j in i['subset']:
            data_dict['dates'].append(j['calendar_date'])
            data_dict['arrays'].append(np.array(j['data']).reshape(meta['nrows'], 
                                                                   meta['ncols']))        
    dtdates = [dt.datetime.strptime(d,"%Y-%m-%d") for d in data_dict['dates']]
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
def request_subset_by_coords(prod, lat, lng, band, start_date, end_date, 
                             ab = 0, lr = 0):
    
    """Get the data from ORNL DAAC by coordinates"""
    
    def getSubsetURL(this_start_date, this_end_date):
        return( "".join([api_base_url, prod, "/subset?",
                     "latitude=", str(lat),
                     "&longitude=", str(lng),
                     "&band=", str(band),
                     "&startDate=", this_start_date,
                     "&endDate=", this_end_date,
                     "&kmAboveBelow=", str(ab),
                     "&kmLeftRight=", str(lr)]))    
            
    if not (isinstance(ab, int) and isinstance(lr, int)): 
        raise TypeError('km_above_below (ab) and km_left_right (lr) must be '
                        'integers!')    
    header = {'Accept': 'application/json'}
    dates = [x['modis_date'] for x in get_product_dates(prod, lat, lng)]
    pydates_arr = np.array([modis_to_from_pydatetime(x) for x in dates])
    start_idx = abs(pydates_arr - start_date).argmin()
    end_idx = abs(pydates_arr - end_date).argmin()
    date_chunks = list(_get_chunks(dates[start_idx: end_idx]))
    subsets = []
    for i, chunk in enumerate(date_chunks):
        print ('[{0} / {1}] {2} - {3}'.format(str(i + 1), str(len(date_chunks)),
               chunk[0], chunk[-1])) 
        url = getSubsetURL(chunk[0], chunk[-1])
        response = requests.get(url, headers = header)
        _error_codes(response)
        subsets.append(json.loads(response.text))
    return _process_data(subsets)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def request_subset_by_siteid(prod, band, network, siteid, start_date, end_date, 
                             filtered = False):
    
    """Get the data from ORNL DAAC by network and site id"""
    
    modis_start = modis_to_from_pydatetime(start_date)
    modis_end = modis_to_from_pydatetime(end_date)
    subset_str = '/subsetFiltered?' if filtered else '/subset?'
    url_str = (''.join([api_base_url, prod, '/', network, '/', siteid, 
                        subset_str, band, '&startDate=', modis_start,
                        '&endDate=', modis_end]))
    header = {'Accept': 'application/json'}
    response = requests.get(url_str, headers = header)
    subset = (json.loads(response.text))
    return _process_data([subset])
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def get_pixel_subset(x_arr, pixels_per_side = 3):
    
    """Create a subset of a larger dataset"""
    
    try:
        assert x_arr.nrows == x_arr.ncols
    except AssertionError: print 'Malformed data array! Exiting...'
    if not x_arr.nrows % 2 != 0:
        raise TypeError('pixels_per_side must be an odd integer!')
    if not pixels_per_side < x_arr.nrows:
        print 'Pixels requested exceeds pixels available!'
        return x_arr
    centre_pixel = x_arr.nrows / 2
    pixel_min = centre_pixel - pixels_per_side / 2
    pixel_max = pixel_min + pixels_per_side
    new_data = []
    for i in range(x_arr.data.shape[2]):
        new_data.append(x_arr.data[pixel_min: pixel_max, pixel_min: pixel_max, i])
    new_x = x_arr.x[pixel_min: pixel_max]
    new_y = x_arr.y[pixel_min: pixel_max]
    attrs_dict = x_arr.attrs.copy()
    attrs_dict['nrows'] = pixels_per_side
    attrs_dict['ncols'] = pixels_per_side
    attrs_dict['xllcorner'] = new_x[0].item()
    attrs_dict['yllcorner'] = new_y[0].item()
    return xr.DataArray(name = x_arr.band,
                        data = np.dstack(new_data),
                        coords = [new_y, new_x, x_arr.time],
                        dims = [ "y", "x", "time" ],
                        attrs = attrs_dict)
#------------------------------------------------------------------------------
    
#------------------------------------------------------------------------------
def interp_and_filter(x_arr, n_points = 11, poly_order = 3):
    
    """Interpolate (Akima) and smooth (Savitzky-Golay) the signal of x_arr"""
    
    # Currently requires some fudging of the data at the start because the 
    # MODIS opendap server is delivering wrong data!
    pd_time = pd.to_datetime(x_arr.time.data[0:len(x_arr.time.data):2])
    n_days = np.array((pd_time - pd_time[0]).days)
    str_data = x_arr.data[1,1,:]
    str_data = str_data[0:len(str_data):2] 
    data = []
    for x in str_data:
        try:
            data.append(float(x))
        except ValueError:
            data.append(np.nan)    
    data = np.array(data)
    valid_idx = np.where(~np.isnan(data))    
    f = interpolate.Akima1DInterpolator(n_days[valid_idx], data[valid_idx])
    interp_series = f(n_days)
    smooth_series = signal.savgol_filter(interp_series, n_points, poly_order, 
                                         mode = "mirror")
    df = pd.DataFrame({'data': data, 'data_interp': interp_series,
                       'data_smooth': smooth_series}, index = pd_time.date)
    return df
#------------------------------------------------------------------------------
