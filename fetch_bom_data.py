# Import standard modules
import sys
sys.path.append('/home/ian/OzFlux/OzFluxQC/scripts')
import csv
import datetime
import glob
import logging
import netCDF4
import numpy
import os
import time
import xlrd
import pdb
import ftplib
import StringIO
import zipfile
import datetime as dt
import copy as cp
import shutil

# Import custom modules
#import constants as c
#import meteorologicalfunctions as mf
#import qcio
#import qcts
#import qcutils
import grunt_email

###############################################################################
# Functions                                                                   #
###############################################################################

#------------------------------------------------------------------------------
def check_line_integrity(line):
    
    # Set values for validity checks
    line_len = 157
    element_n = 33   

    # Do checks
    line_list = line.split(',')
    assert len(line) == line_len # line length consistent?
    assert len(line_list) == element_n # number elements consistent?
    assert '#' in line_list[-1] # hash last character (ex carriage return)?
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def generate_date_list(start_date, end_date):
    
    if not start_date < end_date:
        raise Exception
    delta = end_date - start_date
    count = delta.days * 48 + delta.seconds / 1800 + 1
    return [start_date + dt.timedelta(minutes = i * 30) for
            i in range(count)]
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def generate_dummy_line(valid_line, date):
    
    line_list = valid_line.split(',')
    start_list = ['dd', line_list[1]]
    date_list = [str(date.year), str(date.month).zfill(2), 
                 str(date.day).zfill(2), str(date.hour).zfill(2), 
                 str(date.minute).zfill(2)]
    data_list = [' ' * len(i) for i in line_list[7: -1]]
    dummy_list = start_list + date_list + data_list + [line_list[-1]]
    return ','.join(dummy_list)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def generate_file_copy(old_fpname):
    
    path = os.path.dirname(old_fpname)
    old_fname = os.path.basename(old_fpname)
    new_fname = '{0}.tmp'.format(os.path.splitext(old_fname)[0])
    new_fpname = os.path.join(path, new_fname)
    shutil.copyfile(old_fpname, new_fpname)
    return new_fpname
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
## Strip a sorted list from the sites info file
#def get_bom_id_list(bom_sites_info):
#    
#    bom_id_list = []
#    for key in bom_sites_info.keys():
#        for sub_key in bom_sites_info[key].keys():
#            try:
#                int(sub_key)
#                bom_id_list.append(sub_key)
#            except:
#                continue
#    
#    return sorted(list(set(bom_id_list)))
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
## get the site information and the AWS stations to use
#def get_bom_site_details(path_to_file, sheet_name):
#
#    wb = xlrd.open_workbook(path_to_file)
#    sheet = wb.sheet_by_name(sheet_name)
#    xl_row = 10
#    bom_sites_info = {}
#    for row in range(xl_row,sheet.nrows):
#        xlrow = sheet.row_values(row)
#        flux_site_name = str(xlrow[0])
#        bom_sites_info[flux_site_name] = {}
#        for i, var in enumerate(['latitude', 'longitude', 'elevation']):
#            bom_sites_info[flux_site_name][var] = xlrow[i + 1]
#        for col_idx in [4, 10, 16, 22]:
#            try:
#                bom_site_name = xlrow[col_idx]
#                bom_id = str(int(xlrow[col_idx + 1])).zfill(6)
#                bom_sites_info[flux_site_name][bom_id] = {'site_name': 
#                                                          bom_site_name}
#                for i, var in enumerate(['latitude', 'longitude', 'elevation', 
#                                         'distance']):
#                    bom_sites_info[flux_site_name][bom_id][var] = (
#                        xlrow[col_idx + i + 2])
#            except:
#                continue
#    
#    return bom_sites_info

#------------------------------------------------------------------------------
def get_bom_id(path_to_file, sheet_name):

    wb = xlrd.open_workbook(path_to_file)
    sheet = wb.sheet_by_name(sheet_name)
    header_row = sheet.row_values(9)
    idxs = [i for i, var in enumerate(header_row) if 'BoM ID' in  var]
    start_row = 10
    var_list = []
    for row in range(start_row, sheet.nrows):
        xlrow = sheet.row_values(row)
        for i in idxs:
            try:
                name = str(int(xlrow[i])).zfill(6)
                if not name in var_list:
                    var_list.append(name)
            except:
                continue
    return sorted(var_list)
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# Get the date from a standard line of the BOM data
def get_date_from_line(line, feed_data = False):
    
    i = 1 if feed_data else 0

    line_list = line.split(',')
    date =  dt.datetime(int(line_list[2 + i]), int(line_list[3 + i]), 
                        int(line_list[4 + i]), int(line_list[5 + i]), 
                        int(line_list[6 + i]))
    return date
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# Get the current file list and create a dict
def get_file_id_dict(file_list):
    
    file_list = [i for i in file_list if not 'archive' in i]
    id_list = [j.split('.')[0][:7] for j in [i.split('_')[2] for i in file_list]]
    return dict(zip(id_list, file_list))    
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# Grab the bom data from the ftp server
def get_ftp_data(ftp_server, ftp_dir, req_id_list):

    # Login to ftp server         
    ftp = ftplib.FTP(ftp_server)
    ftp.login()   
    
    # Open the separate zip files and combine in a single zip file 
    # held in dynamic memory - ignore the solar data
    master_sio = StringIO.StringIO() 
    master_zf = zipfile.ZipFile(master_sio, 'w')
    zip_file_list = [os.path.split(f)[1] for f in ftp.nlst(ftp_dir)]   
    for this_file in zip_file_list:
        if 'globalsolar' in this_file: continue
        in_file = os.path.join(ftp_dir, this_file)
        f_str = 'RETR {0}'.format(in_file)
        sio = StringIO.StringIO()
        ftp.retrbinary(f_str, sio.write)
        sio.seek(0)
        zf = zipfile.ZipFile(sio)
        file_list = subset_station_list(zf.namelist(), req_id_list)
        for f in file_list:
            master_zf.writestr(f, zf.read(f))
        zf.close()

    ftp.close()
    master_zf.close()

    return master_sio
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def process_data(z, data_path):
    
    ftp_id_dict = get_file_id_dict(z.namelist())
    current_id_dict = get_file_id_dict(os.listdir(data_path))

    logging.info('Unzipping and processing files:')    

    for site_id in sorted(ftp_id_dict):
        
        # Set up the list for messages to be collated for printing and logging
        msg_list = []
        msg_list.append('BOM station ID {}: '.format(site_id))

        # Open the file and read into memory as a dict (skip to next file if no 
        # - or all corrupt - data)    
        with z.open(ftp_id_dict[site_id], 'r') as bom_f:
            bom_header = bom_f.readline()
            if len(bom_header) == 0:
                msg_list.append('No data found in ftp file; skipping update...')
                logging.warning(''.join(msg_list))
                continue
            bom_dict = {}
            for line in bom_f:
                try:
                    date = get_date_from_line(line, feed_data = True)
                    check_line_integrity(line)
                    new_line = set_line_order(line, bom_header)
                    bom_dict[date] = new_line
                except:
                    continue
        if len(bom_dict) == 0:
            msg_list.append('No valid data found in ftp file; skipping update...')
            logging.warning(''.join(msg_list))
            continue
                    
        # Check if there is a current file for this site id
        if site_id in current_id_dict:

            # If so, read it into memory as a dict
            fname = current_id_dict[site_id]
            current_fpname = os.path.join(data_path, fname)
            with open(current_fpname, 'r') as current_f:
                current_dict = {}
                for i, line in enumerate(current_f):
                    if i == 0:
                        current_header = line
                    else:
                        date = get_date_from_line(line)
                        current_dict[date] = line

            # Create a date list spanning from the beginning of the existing 
            # file to the end of the ftp file, make a temporary copy in case 
            # the process crashes midway through, reopen the file in write mode 
            # and iterate through all dates, writing the line from the relevant 
            # dict, then if the process completes, kill the copy
            temp_fpname = generate_file_copy(current_fpname)
            date_list = generate_date_list(sorted(current_dict.keys())[0],
                                           sorted(bom_dict.keys())[-1])      
            with open(current_fpname, 'w') as out_f:
                out_f.write(current_header)
                for date in date_list:
                    try:
                        line = current_dict[date]
                        assert line[:2] == 'hm'
                    except (KeyError, AssertionError):
                        try:
                            line = bom_dict[date]
                        except KeyError:
                            line = generate_dummy_line(line, date)
                    out_f.write(line)
            os.remove(temp_fpname)
            msg_list.append('Successfully updated file!')
            logging.info(''.join(msg_list))
                            
        else:
            
            # Write all clean lines from BOM file to new file
            msg_list.append('No archive file available... '
                            .format(site_id))
            out_fname = os.path.join(data_path, 
                                     'bom_station_{0}.txt'.format(site_id))
            date_list = sorted(bom_dict.keys())
            with open(out_fname, 'w') as out_f:
                out_f.write(bom_header)
                for date in date_list:
                    line = bom_dict[date]
                    out_f.write(line)
            msg_list.append('Successfully created file!')
            logging.warning(''.join(msg_list))
           
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def set_line_order(line, header):

    def convert_kmh_2_ms(kmh):
        try:
            return str(round(float(kmh) / 3.6, 1)).rjust(5)
        except:
            return kmh.rjust(5)

    swap_dict = {'Wind speed in m/s': 'Wind speed in km/h',
                 'Speed of maximum windgust in last 10 minutes in m/s':
                     'Speed of maximum windgust in last 10 minutes in  km/h'}

    order_dict = {var: i for i, var in enumerate(header.split(','))}

    line_list = line.split(',')

    new_list = []
    for var in header_list:
        try:
            line_idx = order_dict[var]
            value = line_list[line_idx]
        except KeyError:
            alias = swap_dict[var]
            line_idx = order_dict[alias]
            value = convert_kmh_2_ms(line_list[line_idx])
        new_list.append(value)
    new_line = ','.join(new_list)
    return new_line
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def subset_station_list(files_list, target_ID_list):
    
    unq_files_list = sorted(list(set([f for f in files_list if 'Data' in f])))
#    f_names_list = []
#    counter = 0
    avail_list = [f.split('_')[2] for f in unq_files_list]
    common_list = list(set(avail_list).intersection(target_ID_list))
    file_name_list = []
    for f in common_list:
        idx = avail_list.index(f)
        file_name_list.append(unq_files_list[idx])
    return file_name_list
#
#    for ID in target_ID_list:
#        for f_name in unq_files_list[counter:]:
#            if '009053' in f_name and ID == '009053':
#                pdb.set_trace()
#            if ID in f_name:
#                f_names_list.append(f_name)
#                counter = unq_files_list.index(f_name)
#                break     
#    pdb.set_trace()
#    return f_names_list
#------------------------------------------------------------------------------

###############################################################################
# Main program                                                                #
###############################################################################

# Set stuff up
ftp_server = 'ftp.bom.gov.au'
ftp_dir = 'anon2/home/ncc/srds/Scheduled_Jobs/DS010_OzFlux/'
xlname = "/mnt/OzFlux/AWS/AWS_Locations.xls"
data_path = "/mnt/OzFlux/AWS/Test/"
mail_recipients = ['ian_mchugh@fastmail.com']

header_list = ['hm',
               'Station Number',
               'Year Month Day Hour Minutes in YYYY',
               'MM',
               'DD',
               'HH24',
               'MI format in Local time',
               'Year Month Day Hour Minutes in YYYY',
               'MM',
               'DD',
               'HH24',
               'MI format in Local standard time',
               'Precipitation since 9am local time in mm',
               'Quality of precipitation since 9am local time',
               'Air Temperature in degrees C',
               'Quality of air temperature',
               'Dew point temperature in degrees C',
               'Quality of dew point temperature',
               'Relative humidity in percentage %',
               'Quality of relative humidity',
               'Wind speed in m/s',
               'Wind speed quality',
               'Wind direction in degrees true',
               'Wind direction quality',
               'Speed of maximum windgust in last 10 minutes in m/s',
               'Quality of speed of maximum windgust in last 10 minutes',
               'Station level pressure in hPa',
               'Quality of station level pressure',
               'AWS Flag',
               '#\r\n']

# Set up logging
t = time.localtime()
rundatetime = (datetime.datetime(t[0],t[1],t[2],t[3],t[4],t[5])
               .strftime("%Y%m%d%H%M"))
log_filename = '/home/imchugh/Temp/logfiles/aws_data_'+rundatetime+'.log'    
logging.basicConfig(filename=log_filename,
                    format='%(levelname)s %(message)s',
                    #datefmt = '%H:%M:%S',
                    level=logging.DEBUG)
console = logging.StreamHandler()
formatter = logging.Formatter('%(levelname)s %(message)s')
#formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s', 
#                              '%H:%M:%S')
console.setFormatter(formatter)
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

# dummy control file for FixTimeSteps
cf = {"Options":{"FixTimeStepMethod":"round"}}

logging.info('Run date and time: {}'.format(rundatetime))

try:
    # get bom site details
#    bom_sites_info = get_bom_site_details(xlname, 'OzFlux')
#    bom_id_list = get_bom_id_list(bom_sites_info)
    bom_id_list = get_bom_id(xlname, 'OzFlux')

    # Get the available data from the ftp site and cross-check against request
    sio = get_ftp_data(ftp_server, ftp_dir, bom_id_list)
    z = zipfile.ZipFile(sio)
    ftp_id_dict = get_file_id_dict(z.namelist())
    missing_from_ftp = ', '.join(list(set(bom_id_list)-set(ftp_id_dict.keys())))
    logging.warning('The following requested BOM site IDs were missing from ftp site: {0}'
                    .format(missing_from_ftp))
    # Process the data
    process_data(z, data_path)
    z.close()
    grunt_email.email_send(mail_recipients, 'BOM data processing status', 
                     'Successfully collected and processed data for BOM stations '
                     '(see log for details)')
except Exception, e:
    pdb.set_trace()
    grunt_email.email_send(mail_recipients, 'BOM data processing status', 
                     'Data processing failed with the following message {0} '
                     .format(e))
