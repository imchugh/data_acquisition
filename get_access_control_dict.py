#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jul 31 16:22:39 2017

@author: ian
"""

import xlrd
import os
from timezonefinder import TimezoneFinder as tzf

###############################################################################
# Functions                                                                   #
###############################################################################

#------------------------------------------------------------------------------
def get_control_dict(base_dir, yearmonth, master_file_path):

    ### Define working and final dictionaries ###
    
    # ACCESS variable name alias dictionary
    access_alias_dict = {'Fsd': 'av_swsfcdown',
                         'Fn_sw': 'av_netswsfc',
                         'Fld': 'av_lwsfcdown',
                         'Fn_lw': 'av_netlwsfc',
                         'Ta': 'temp_scrn',
                         'q': 'qsair_scrn',
                         'Sws': 'soil_mois',
                         'Ts': 'soil_temp',
                         'u': 'u10',
                         'v': 'v10',
                         'ps': 'sfc_pres',
                         'Precip': 'accum_prcp',
                         'Fh': 'sens_hflx',
                         'Fe': 'lat_hflx',
                         'Habl': 'abl_ht'}
    
    # Site and file naming alias dictionary
    site_alias_dict = {'Emerald': {'dict_name': 'Emerald',
                                   'site_name': 'Emerald',
                                   'in_filename': 'Arcturus',
                                   'out_filename': 'Emerald'},
                       'Yanco': {'dict_name': 'Jaxa',
                                 'site_name': 'Yanco',
                                 'in_filename': 'Jaxa',
                                 'out_filename': 'Yanco'}}
    
    # Final dictionary
    master_dict = {'Sites': {},
                   'Variables': {},
                   'Options': {}}
    
    ### Build the 'Sites' control dicts ###
        
    # Get the data from the master file and initialise location list for timezone
    wb = xlrd.open_workbook(master_file_path)
    sheet = wb.sheet_by_name('Active')
    site_list = [str(site.value) for site in sheet.col(0, 10)]
    lat_list = [site.value for site in sheet.col(4, 10)]
    lon_list = [site.value for site in sheet.col(5, 10)]
    loc_d = {site[0]: site[1:] for site in zip(site_list, lat_list, lon_list)}
    
    # Iterate over sites
    for site_name in site_list:
           
        this_dict = {}
        
        site_loc = loc_d[site_name]
        this_dict['site_timezone'] = tzf().timezone_at(lng = site_loc[1], 
                                                       lat = site_loc[0])
        
        try:
            this_alias_dict = site_alias_dict[site_name]
            site_name = this_alias_dict['site_name']
            in_filename_part = this_alias_dict['in_filename'].replace(' ', '_')
            out_filename_part = this_alias_dict['out_filename'].replace(' ', '')
            dict_name = this_alias_dict['dict_name'].replace(' ', '')
        except KeyError:
            in_filename_part = site_name.replace(' ', '_')
            out_filename_part = site_name.replace(' ', '')
            dict_name = out_filename_part
        this_dict['in_filename'] = '{}*.nc'.format(in_filename_part)
        this_dict['out_filename'] = '{0}_ACCESS_{1}.nc'.format(out_filename_part, 
                                                               yearmonth)
        this_dict['in_filepath'] = os.path.join(base_dir, yearmonth + '/')
        this_dict['out_filepath'] = os.path.join(base_dir, 'monthly', yearmonth + '/')
        this_dict['interpolate'] = 'True'
        this_dict['site_name'] = site_name
        
        master_dict['Sites'][dict_name] = this_dict
    
    ### Build the 'Variables' control dict
    for key in access_alias_dict:
        master_dict['Variables'][key] = {'access_name': access_alias_dict[key]}
    
    ### Build the options dict
    master_dict['Options'] = {'WriteExcelIntermediate' : 'No',
                              'WriteExcelNoGaps': 'No'}
    
    return master_dict
#------------------------------------------------------------------------------

###############################################################################
# Main                                                                        #
###############################################################################


base_dir = '/rdsi/market/access_opendap'
yearmonth = '201703'
master_file_path = '/home/ian/Temp/site_master.xls'

master_dict = get_control_dict(base_dir, yearmonth, master_file_path)