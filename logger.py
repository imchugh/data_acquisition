#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 18 13:36:05 2018

@author: ian
"""

import datetime as dt
import logging
import os
import time

def set_logger(logfile_path, file_prefix):
    '''
    Set up generic logger
    Args:
        * logfile_path (str): directory to write the log file to
        * file_prefix (str): appends this string to beginning of file name
          (which otherwise consists of date and time)
    '''
    
    t = time.localtime()
    rundatetime = (dt.datetime(t[0],t[1],t[2],
                               t[3],t[4],t[5]).strftime("%Y%m%d%H%M"))
    log_filename = os.path.join(logfile_path, '{0}_{1}.log'.format(file_prefix, 
                                                                   rundatetime))    
    logging.basicConfig(filename = log_filename,
                        format = '%(levelname)s %(message)s',
                        #datefmt = '%H:%M:%S',
                        level=logging.DEBUG)
    console = logging.StreamHandler()
    formatter = logging.Formatter('%(levelname)s %(message)s')
    console.setFormatter(formatter)
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)