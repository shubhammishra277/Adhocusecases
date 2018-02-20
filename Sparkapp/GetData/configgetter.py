'''
Created on 19-Feb-2018

@author: shubham
'''
import sys
from loggermodule import logger_test
from ConfigParser import RawConfigParser

class configparser(object):
    
        def configparse(self,client_name):
             logger_test.debug("parsing the configuration file for processing the data")
             try:
              parser =RawConfigParser()
              parser.read('config.ini')
              m=parser.items(client_name)
             #print "m",m
              consumer_key=m[0][1]
             
              consumer_secret=m[1][1]
              access_token_key=m[2][1]
              access_token_secret=m[3][1]
             except Exception,e:
                 logger_test.exception("error occured while reading the config file with error :%s"%str(e))
                 
             return client_name,consumer_key,consumer_secret,access_token_key,access_token_secret


if __name__=="__main__":
    #clientname_value=sys.argv[1].split("=")[-1]
    clientname_value="Shubham"
    t1=configparser()
    client_name,consumer_key,consumer_secret,access_token_key,access_token_secret=t1.configparse(clientname_value)
    