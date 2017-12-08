from googleads import adwords 
from datetime import datetime
import time
import multiprocessing
import csv
import ast
import os
from Yamlfilescreator import creator
from loggermodule import logger_test
import glob
import pandas as pd
import datetime

'''############################### FUNCTIONS ################################'''
from ConfigParser import RawConfigParser


def parse():
      parser = RawConfigParser()
      parser.read('accounts_config.ini')
      client_name=parser.sections().pop()
      m=parser.items(client_name)
      timeframe=m[6][1]
      filepath=m[7][1]
      timedefination=eval(m[8][1])
      return timeframe,filepath,timedefination
def ConnectToNewClient(AccountID):

    BASE_DIR = os.getcwd()
    DOCUMENTS_DIR = BASE_DIR
    global ADWORDS_CLIENT 

    DIR = DOCUMENTS_DIR + '/YamlFiles/googleads_' + AccountID + '.yaml'
    ADWORDS_CLIENT = adwords.AdWordsClient.LoadFromStorage(DIR)
   
    
def GetKeywordURLs(val):
      report_downloader = ADWORDS_CLIENT.GetReportDownloader()
      timevalue,file_path,defination_value=parse()
      BASE_DIR = os.getcwd()

      DOCUMENTS_DIR = BASE_DIR
      report = {
            'reportName': 'URL finales de la cuenta',
            'dateRangeType': '%s'%timevalue,
            'reportType': 'KEYWORDS_PERFORMANCE_REPORT',
            'downloadFormat': 'CSV',
            'selector': {
                'fields': ['ExternalCustomerId','AccountDescriptiveName','CampaignName','CampaignStatus','AdGroupName','AdGroupStatus','Criteria','Status','KeywordMatchType','Clicks','Impressions','Conversions','Cost'],
                'predicates': [
                    {
                    'field':'CampaignName',
                    'operator':'DOES_NOT_CONTAIN',        
                    'values':'GDN'},
                    {
                      'field': 'Status',
                      'operator': 'IN',
                      #'values': ['ENABLED','PAUSED']},
                      'values': ['ENABLED']},
                  
                     {
                      'field': 'AdGroupStatus',
                      'operator': 'IN',
                      #'values': ['ENABLED','PAUSED']},
                      'values': ['ENABLED']},
       
                    
                     {
                      'field': 'CampaignStatus',
                      'operator': 'IN',
                      #'values': ['ENABLED','PAUSED']}
                      'values': ['ENABLED']}

                    ]
            }
        }

      CSV_key_File = open(DOCUMENTS_DIR + '/KeyReport_%s.csv'%val,'wb')
      report_downloader.DownloadReport(
          report, CSV_key_File, skip_report_header=True, skip_column_header=False,
          skip_report_summary=True, include_zero_impressions=True)
      CSV_key_File.close()
def filegenerator(account,account_no):
    try:
       logger_test.info("starting for this account id :%s"%account)
       ConnectToNewClient(account)
       GetKeywordURLs(account)
    except :
       logger_test.exception("error occured for %s "%account)


    logger_test.info("This process is complete")
def processopener(path):
    print "inside process opener"
    k=[i.split(".")[0].split("_")[-1] for i in glob.glob("%s/YamlFiles/*.yaml"%path)]
    logger_test.info("List of accounts %s"%k)
    process=["p"+str(i) for i in range(len(k))]
    logger_test.info("no. of Yaml files:%s"%len(k))
    for y in range(len(k)):

        process[y]=multiprocessing.Process(target=filegenerator, args=(k[y],y,))

    for y in process:
           y.start()

    while True:
      time.sleep(1)
      if not multiprocessing.active_children():
        break
     
def aggregator12():
      os.system("rm -r YamlFiles/")
      creator()
      path=os.getcwd()
      print "path",path
      processopener(path)
      keyreport=[i for i in glob.glob("%s/KeyReport_*.csv"%path) if os.path.getsize(i)!=0]
      logger_test.info("doing aggregation to create a single Keyreport file")
      all_key=pd.DataFrame()
      for i in keyreport:
             df = pd.read_csv(i)
             all_key = all_key.append(df,ignore_index=True)
      all_key=all_key[~all_key.Campaign.str.contains('GSP|Display|Search Companion|In-Stream|In-Market|RLSA')==True]
      all_key.to_csv("%s/KeyReport.csv"%path,index=False)
      os.system("rm %s/KeyReport_*.csv"%path)
if __name__=="__main__":
        
          aggregator12()
