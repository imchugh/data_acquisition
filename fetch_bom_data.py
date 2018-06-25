# Import standard modules
import datetime as dt
import ftplib
import logging
import os
import pdb
import shutil
import StringIO
import time
import xlrd
import zipfile
from pytz import timezone
from timezonefinder import TimezoneFinder as tzf
import pdb

# Import custom modules
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
def check_process_complete(path):

    tmp_list = [f for f in os.listdir(path) if '.tmp' in f]
    if len(tmp_list) == 0:
        return
    logging.warning('The following files were not processed to completion on '
                    'the previous run: {}; reverting to uncorrupted copy!'
                    .format(', '.join(tmp_list)))
    for f in tmp_list:
        f_tuple = os.path.splitext(f)
        old_f_name = os.path.join(path, f)        
        new_f_name = os.path.join(path, '{}.csv'.format(f_tuple[0]))
        if os.path.isfile(new_f_name):
            os.remove(new_f_name)
        os.rename(old_f_name, new_f_name)
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
def generate_dummy_line(valid_line, date, tz):
    
    line_list = valid_line.split(',')
    start_list = ['dd', line_list[1]]
    local_datetime = get_local_datetime(date, tz)
    local_date_list = [str(local_datetime.year), 
                       str(local_datetime.month).zfill(2), 
                       str(local_datetime.day).zfill(2), 
                       str(local_datetime.hour).zfill(2), 
                       str(local_datetime.minute).zfill(2)]
    std_date_list = [str(date.year), str(date.month).zfill(2), 
                     str(date.day).zfill(2), str(date.hour).zfill(2), 
                     str(date.minute).zfill(2)]
    data_list = [' ' * len(i) for i in line_list[12: -1]]
    dummy_list = (start_list + local_date_list + std_date_list + data_list + 
                  [line_list[-1]])
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
def get_bom_id_dict(file_path):
    
    wb = xlrd.open_workbook(file_path)
    sheet = wb.sheet_by_name('Active')
    header_row = 9
    header_list = sheet.row_values(header_row)
    start_row = 10
    id_dict = {}
    for row_num in range(start_row, sheet.nrows):
        this_row = sheet.row_values(row_num)
        for i in range(1, 5):
            stn_col_idx = header_list.index('BoM_ID_{}'.format(str(i)))
            lat_col_idx = header_list.index('Latitude_{}'.format(str(i)))
            lon_col_idx = header_list.index('Longitude_{}'.format(str(i)))
            try:
                bom_id = str(int(this_row[stn_col_idx])).zfill(6)
                id_dict[bom_id] = {}
                latitude = this_row[lat_col_idx]
                longitude = this_row[lon_col_idx]
                tz = tzf().timezone_at(lng = longitude, lat = latitude)
                id_dict[bom_id]['latitude'] = latitude
                id_dict[bom_id]['longitude'] = longitude
                id_dict[bom_id]['time_zone'] = tz
            except:
                continue
    return id_dict
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# Get the date from a standard line of the BOM data
def get_date_from_line(line, feed_data = False):
    
    i = 1 if feed_data else 0

    line_list = line.split(',')
    date =  dt.datetime(int(line_list[7 + i]), int(line_list[8 + i]), 
                        int(line_list[9 + i]), int(line_list[10 + i]), 
                        int(line_list[11 + i]))
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
def get_local_datetime(dt_obj, tz_name):
    
    tz_obj = timezone(tz_name)
    try:
        dst_offset = tz_obj.dst(dt_obj)
        prev_offset = tz_obj.dst(dt_obj - dt.timedelta(seconds = 3600))
    except:
        dst_offset = tz_obj.dst(dt_obj + dt.timedelta(seconds = 3600))
    return dt_obj + dst_offset 
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
def process_data(z, data_path):
    
    ftp_id_dict = get_file_id_dict(z.namelist())
    current_id_dict = get_file_id_dict(os.listdir(data_path))

    logging.info('Unzipping and processing files:')    

    for site_id in sorted(ftp_id_dict):
        
        tz = bom_id_dict[site_id]['time_zone']
        
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
                        try:
                            date = get_date_from_line(line)
                        except:
                            continue
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
                            assert line[:2] == 'hm'
                        except (KeyError, AssertionError):
                            line = generate_dummy_line(line, date, tz)
                    out_f.write(line)
            os.remove(temp_fpname)
            msg_list.append('Successfully updated file!')
            logging.info(''.join(msg_list))
                             
        else:
            
            # Write all clean lines from BOM file to new file
            msg_list.append('No archive file available... '
                            .format(site_id))
            out_fname = os.path.join(data_path, 
                                     'HM01X_Data_{0}.csv'.format(site_id))
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
    avail_list = [f.split('_')[2] for f in unq_files_list]
    common_list = list(set(avail_list).intersection(target_ID_list))
    file_name_list = []
    for f in common_list:
        idx = avail_list.index(f)
        file_name_list.append(unq_files_list[idx])
    return file_name_list
#------------------------------------------------------------------------------

###############################################################################
# Main program                                                                #
###############################################################################

# Set stuff up
ftp_server = 'ftp.bom.gov.au'
ftp_dir = 'anon2/home/ncc/srds/Scheduled_Jobs/DS010_OzFlux/'
xlname = '/mnt/OzFlux/Sites/site_master.xls'
data_path = "/rdsi/market/aws_ftp"
logfile_path = "/mnt/OzFlux/test_code/data_acquisition/Logfiles/AWS"
mail_recipients = ['ian.mchugh@monash.edu']

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
rundatetime = (dt.datetime(t[0],t[1],t[2],t[3],t[4],t[5]).strftime("%Y%m%d%H%M"))
log_filename = os.path.join(logfile_path, 'aws_data_{}.log'.format(rundatetime))    
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
    # Check for failed processing on previous run
    check_process_complete(data_path)

    # get bom site details
    bom_id_dict = get_bom_id_dict(xlname)
    bom_id_list = sorted(bom_id_dict.keys())

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

    msg = 'Successfully collected and processed data for BOM stations!'
    logging.info(msg)
    grunt_email.email_send(mail_recipients, 'BOM data processing status', msg)
except Exception, e:
    msg = ('Data processing failed with the following message {0}'.format(e))
    logging.info(msg)
    grunt_email.email_send(mail_recipients, 'BOM data processing status', msg)
