import logging
import datetime
import os
from ConfigParser import RawConfigParser

parser = RawConfigParser()
parser.read('config.ini')
client_name="LoggingLevel"
m=parser.items(client_name)
logginglevel=str.upper(m[0][1])
LEVELS = {'DEBUG': logging.DEBUG,
          'INFO': logging.INFO,
          'WARNING': logging.WARNING,
          'ERROR': logging.ERROR,
          'CRITICAL': logging.CRITICAL}



logger_test=logging.getLogger(__name__)
current_date=datetime.date.today().isoformat()

logger_test.setLevel(LEVELS[logginglevel])
p=os.getcwd()
os.system("mkdir -p %s/logs"%p)
ch = logging.FileHandler('%s/logs/datalogs_%s.log'%(p,current_date),mode='a')
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s  - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger_test.addHandler(ch)
#print logger_test
