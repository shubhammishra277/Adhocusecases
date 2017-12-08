#!/usr/bin/env python
# -*- coding: iso-8859-15 -*-

'''################################ METADATA ################################'''
__authors__ = 'Shubham Mishra'
__maintainer__ = 'Shubham Mishra'
__email__ = "shubham.mishra@ogilvy.com"

'''############################### LIBRARIES ################################'''
from oauth2client.service_account import ServiceAccountCredentials
#from oauth2client.client import SignedJwtAssertionCredentials
from datetime import datetime
import gspread
import json
import sys
import os
from loggermodule import logger_test
import httplib2

    
def ConnectToDrive():
    
    DOCUMENTS_DIR =os.getcwd()
    json_key = json.load(open(DOCUMENTS_DIR + '/DriveValidation.json'))
    scope = ['https://spreadsheets.google.com/feeds']

    credentials = ServiceAccountCredentials.from_json_keyfile_name(DOCUMENTS_DIR + '/DriveValidation.json', scope)
    http = httplib2.Http()
    credentials.authorize(http) 
    DRIVE_SPREAD_CLIENT = gspread.authorize(credentials)
    logger_test.info('Connected to drive')
    return DRIVE_SPREAD_CLIENT




'''################################ PROGRAM #################################'''
if __name__ == '__main__':
    ConnectToDrive()
    logger_test.info('!! ALL DONE !!')

