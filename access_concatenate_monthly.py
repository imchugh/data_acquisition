# standard
from collections import OrderedDict
from configobj import ConfigObj
import datetime
import glob
import netCDF4
import os
import pdb
import sys
import time
# 3rd party
import xlrd
import xlwt
# since the scripts directory is there, try importing the modules
sys.path.append('/mnt/PyFluxPro/scripts')
# PFP
import qcio
import qclog

t = time.localtime()
rundatetime = datetime.datetime(t[0],t[1],t[2],t[3],t[4],t[5]).strftime("%Y%m%d%H%M")
log_filename = 'access_concatenate_'+rundatetime+'.log'
logger = qclog.init_logger(logger_name="pfp_log", file_handler=log_filename)

def read_site_master(xl_file_path, sheet_name):
    """
    """
    xl_book = xlrd.open_workbook(xl_file_path)
    xl_sheet = xl_book.sheet_by_name(sheet_name)
    last_row = int(xl_sheet.nrows)
    # find the header and first data rows
    for i in range(last_row):
        if xl_sheet.cell(i,0).value == "Site":
            header_row = i
            first_data_row = header_row + 1
            break
    # read the header row
    header_row_values = xl_sheet.row_values(header_row)
    # read the site data from the master Excel spreadsheet
    site_info = OrderedDict()
    for n in range(first_data_row,last_row):
        site_name = xl_sheet.cell(n,0).value
        site_name = site_name.replace(" ","")
        site_info[site_name] = OrderedDict()
        for item in header_row_values[1:]:
            i = header_row_values.index(item)
            site_info[site_name][item] = xl_sheet.cell(n,i).value

    return site_info

xl_file_path = '/mnt/OzFlux/Sites/site_master.xls'
sheet_name = 'Active'
nc_base_path = "/mnt/OzFlux/Sites/"
access_base_path = "/rdsi/market/access_opendap/monthly"

logger.info("Getting a list of months available for concatenation")
access_monthly_dir_list = sorted(glob.glob(access_base_path+"/*"))
access_month_full_list = [item.split("/")[-1] for item in access_monthly_dir_list]

# read the site master file and get a list of sites to process
logger.info("Reading the site master file")
site_info = read_site_master(xl_file_path, sheet_name)
site_list = site_info.keys()
for site in site_list:
    logger.info("Processing site "+site)
    path_to_site_nc = os.path.join(nc_base_path, site, 'Data/ACCESS', 
                                   '{}_ACCESS.nc'.format(site))
    try:
        ncobj = netCDF4.Dataset(path_to_site_nc)
        last_date = netCDF4.num2date(ncobj.variables['time'][-1],
                                     'days since 1800-01-01 00:00:00')
        ncobj.close()
        processed_to_month = datetime.datetime.strftime(last_date, '%Y%m')
        try:
            idx = access_month_full_list.index(processed_to_month)
            months_to_process = access_month_full_list[idx:]
        except ValueError:
            'No new monthly data available for concatenation!'
            months_to_process = []
    except:
        continue
    pdb.set_trace()
    if not len(months_to_process) == 0:
        access_file_path_list = []
        for n, access_month in enumerate(months_to_process):
            access_file_path = os.path.join(access_base_path,access_month,site+"_ACCESS_"+access_month+".nc")
            if os.path.exists(access_file_path):
                access_file_path_list.append(access_file_path)
        if len(access_file_path_list) > 0:
            config_dict = {}
            config_dict["Options"] = {"NumberOfDimensions":1,
                                      "MaxGapInterpolate":0,
                                      "FixTimeStepMethod":"round",
                                      "Truncate":"No",
                                      "TruncateThreshold":50,
                                      "SeriesToCheck":[]}
            nc_file_name = site+"_ACCESS.nc"
            nc_out_path = os.path.join(nc_base_path,site,"Data","ACCESS",nc_file_name)
            config_dict["Files"] = {"Out":{"ncFileName":nc_out_path},"In":{}}
            config_dict["Files"]["In"]["0"] = nc_out_path
            for n, access_file_path in enumerate(access_file_path_list):
                config_dict["Files"]["In"][str(n+1)] = access_file_path

logger.info("Finished generating ACCESS concatenation control files")
# now do the concatenation
file_list = sorted(glob.glob(cf_base_path+"/*"))
for item in file_list:
    cf_read = ConfigObj(infile=item)
    cf_path, cf_name = os.path.split(item)
    logger.info("Concatenating using "+cf_name)
    qcio.nc_concatenate(cf_read)

logger.info("")
logger.info("access_concatenate: all done")
