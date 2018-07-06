#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Mon Jun 18 16:18:30 2018

@author: ian
"""

import requests



api_key = '50CtkoB1FHWW9DQUknhqZbplhlAXPx8JB004rx63'

params = {'apikey': {api_key},
          'format': 'json',
          'station': 1001,
          'start': '20160101',
          'finish': '20160105',
          'variables': 'max_temp,min_temp'}

r = requests.get('https://siloapi.longpaddock.qld.gov.au/pointdata', params=params)
point_data = r.json()
print(point_data)

r = requests.get('https://siloapi.longpaddock.qld.gov.au/variables')
variables = r.json()
print(variables)