#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jul  3 12:16:33 2017

@author: ian
"""

import xlrd
import os

xlname = "/mnt/OzFlux/AWS/AWS_Locations.xls"
#archive_fpname = '/home/ian/Downloads/HM01X_Data_009053.csv'
#outfile_fpname = '/home/ian/Temp/output.csv'

# get the site information and the AWS stations to use
def get_bom_site_details(path_to_file, sheet_name):

    wb = xlrd.open_workbook(path_to_file)
    sheet = wb.sheet_by_name(sheet_name)
    xl_row = 10
    bom_sites_info = {}
    for row in range(xl_row,sheet.nrows):
        xlrow = sheet.row_values(row)
        flux_site_name = str(xlrow[0])
        bom_sites_info[flux_site_name] = {}
        for i, var in enumerate(['latitude', 'longitude', 'elevation']):
            bom_sites_info[flux_site_name][var] = xlrow[i + 1]
        for col_idx in [4, 10, 16, 22]:
            try:
                bom_site_name = xlrow[col_idx]
                bom_id = str(int(xlrow[col_idx + 1])).zfill(6)
                bom_sites_info[flux_site_name][bom_id] = {'site_name': 
                                                          bom_site_name}
                for i, var in enumerate(['latitude', 'longitude', 'elevation', 
                                         'distance']):
                    bom_sites_info[flux_site_name][bom_id][var] = (
                        xlrow[col_idx + i + 2])
            except:
                continue
    
    return bom_sites_info

sites_dict = get_bom_site_details(xlname, 'OzFlux')
id_name_dict = {}
for key in sites_dict.keys():
    for sub_key in sites_dict[key].keys():
        try:
            int(sub_key)
            id_name_dict[sub_key] = sites_dict[key][sub_key]['site_name']
        except:
            pass
id_name_dict['005008'] = 'Mardie'
id_name_dict['007176'] = 'Newman Aero'
id_name_dict['007185'] = 'Paraburdoo Aero'

in_path = '/mnt/OzFlux/AWS/Current'
out_path = '/mnt/OzFlux/AWS/Test'
f_list = os.listdir(in_path)

for f_name in f_list:

    fname_part = os.path.splitext(f_name)[0]
    this_id = fname_part.split('_')[2]

    try:
        station_name = id_name_dict[this_id].ljust(40)
    except KeyError:
        print 'No dictionary entry for file {0}'.format(this_id)
        continue

    in_fpname = os.path.join(in_path, f_name)
    out_fpname = os.path.join(out_path, f_name)
    with open(in_fpname, 'r') as in_f, open(out_fpname, 'w') as out_f:
        header = in_f.readline()
        header_list = header.split(',')
        new_header_list = header_list[:2] + ['Station Name'] + header_list[7:]
        new_header=','.join(new_header_list)
        out_f.write(new_header)
        for line in in_f:
            line_list = line.split(',')
            new_line_list = line_list[:2] + [station_name] + line_list[7:]
            new_line = ','.join(new_line_list)
            out_f.write(new_line)
