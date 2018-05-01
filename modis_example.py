#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue May  1 13:51:07 2018

@author: ian
"""

import modis_functions as mf

master_file_path = '/home/ian/Temp/site_master.xls'
site = 'Warra'
product = 'MOD15A2H'
band = 'Lai_500m'
lat = -36.673050
lon = 145.026214
start_date = dt.datetime(2013, 1, 1)
end_date = dt.datetime(2014, 1, 1)

###############################################################################
# Utility functions
###############################################################################

# Find what products are available from the web service
prod_list = mf.get_product_list()
print prod_list

# Find what bands are available for a given product
band_list = mf.get_band_list(product)
print 'Available bands for {0}: {1}'.format(product, ', '.join(band_list))

# Find what dates are available for a given product
date_list = mf.get_date_list(lat = lat, lon = lon, product = product)
print 'Available dates for {0}: {1}'.format(product, ', '.join(date_list))

# Find the web page for the product
mf.get_product_web_page(product)

# Get the qc bitmap for the product
bitmap = mf.get_qc_bitmap(product)
print 'Bitmap for {0} is: {1}'.format(product, bitmap)

# Get the definitions of qc flags for the product
qc_dictionary = mf.get_qc_definition(product)
print qc_dictionary

# Get the threshold for acceptable data
qc_threshold =mf.get_qc_threshold(product)
print 'QC threshold for {0} is: {1}'.format(product, str(qc_threshold))

# Get the QC band for a given product
qc_band = mf.get_qc_variable_band(product)
print 'The qc_variable for product {0} is {1}'.format(product, ', '.join(qc_band))

# Extract the list of site latitudes and longitudes from the site_master file
mf.get_ozflux_site_list(master_file_path)

###############################################################################
# Data classes
###############################################################################

# Generic class
x = mf.modis_data(lat, lon, product, band, start_date, end_date)

# Container for generating ozflux site data
x_container = mf.ozflux_data_generator(master_file_path)

# Specific OzFlux class (returns same object as generic class, but drops the 
#                        need to pass latitude and longitude information)
warra = x_container.ozflux_modis_data(site, product, band, start_date, end_date)

# Plot the data
warra.plot_data()

# Look at the data (pandas dataframe)
warra.data

# Look at some other attributes (see modis_functions docstring for further 
#                                attributes)
warra.cellsize
warra.units
warra.ncols
warra.nrows