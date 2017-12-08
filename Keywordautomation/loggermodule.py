import logging 
import datetime
import os
logger_test=logging.getLogger(__name__)
current_date=datetime.date.today().isoformat()
logger_test.setLevel(logging.DEBUG)
p=os.getcwd()
os.system("mkdir -p %s/logs"%p)
ch = logging.FileHandler('%s/logs/keywordreport_%s.log'%(p,current_date),mode='a')
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s  - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger_test.addHandler(ch)
