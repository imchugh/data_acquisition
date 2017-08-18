#!/usr/bin/env python
#
# downloading Sat Soil Moisture data from NASA site
#
# Usage:  python get_data.py [YYYYMMDD]
#                (default is yesterday's date)

import os, sys
from subprocess import call as spc
from datetime import date, timedelta
import pdb
import xlrd

#####
prot = 'http://'
svr = 'opendap.bom.gov.au:8080'
pth = '/thredds/fileServer/bmrc/access-r-fc/ops/surface/'

tmpfile = './temp/access.nc'
site_list = '/rdsi/market/access_opendap/sites_list.txt'

# check if date is on command line - else default to yesterday
args = sys.argv[1:]
if len(args) < 1:
    yest = date.today() - timedelta(1)
    ymd = yest.strftime('%Y%m%d')
else:
    ymd = args[0]

#print ymd
#exit(0)

#ymd = '20150514'
ym = ymd[:6]
hrs = ['00', '06', '12', '18']
pref = '/ACCESS-R_' 
suff = '_surface.nc'

delta = 0.165

# create destination dir
if not os.path.exists(ym):
    print 'Creating dir: ', ym
    os.makedirs(ym)

# formulate wget command
#wget = '/usr/bin/wget -nv -N -nH -np -a Download.log -P ' + ym + ' -0 ' + tmpfile + ' '
wget = '/usr/bin/wget -nv -a Download.log -O ' + tmpfile + ' '

# read in site lat/lon
with open(site_list, 'r') as fin:
    sites = list(fin)
pdb.set_trace()
#####
#for h in hrs:
#    for t in range(7):
#        access = pref + ymd + h + '_' + str(t).zfill(3) + suff
#        cmd = wget + prot + svr + pth + ymd + h + access
#        print cmd
#        if spc(cmd, shell=True):
#            print 'Error in command: ', cmd
#        else:
#            # extract data for the sites, then can throw away the tmpfile
#        #if True:
#            for sl in sites:
#                sname, slat, slon = sl.split(',')
#               sname = sname.replace(' ', '_')
#                lat = float(slat)
#                lon = float(slon.strip())
#                #print sname, lat, lon
#                ncks = '/usr/bin/ncks -d lat,' + str(lat - delta) + ',' + str(lat + delta)  + ' -d lon,' \
#                     + str(lon - delta) + ',' + str(lon + delta)  + ' ' \
#                     + tmpfile + ' ' + ym + '/' + sname + '_' + ymd + h + '_' + str(t).zfill(3) + '.nc'
#                print ncks
#                if spc(ncks, shell=True):
#                    print 'Error in command: ', ncks
#           # 
#            os.remove(tmpfile)
 

#print ' --- All done ---'
 
