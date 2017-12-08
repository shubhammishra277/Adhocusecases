import datetime
import mysql.connector
import os
import glob 
import pandas as pd
from loggermodule import logger_test
from keywords_bak import aggregator12 
from ConfigParser import RawConfigParser

class monthlydata(object):
   
       def  __init__(self):
                databasename,password,databaseuser=self.parse()
                self.cnx = mysql.connector.connect(user='%s'%databaseuser, password='%s'%password,
                              #host='127.0.0.1',
                              database='%s'%databasename)
                self.handler = self.cnx.cursor(buffered=True)
                logger_test.info("Connected to local Database")
     
       def parse(self):

           parser = RawConfigParser()
           parser.read('accounts_config.ini')
           client_name=parser.sections().pop()
           m=parser.items(client_name)
           database=m[9][1]
           password=m[10][1]
           user=m[11][1]
           return database,password,user

       def previousdatadelete(self):
                   
                   self.handler.execute("Truncate table Keyreport")
                   logger_test.info("deleted old data")
       def loadnewdata(self):
                   logger_test.info("Downloading keyword report data")
                   filetogen=[i for i in glob.glob("KeyReport.csv")]
                   if len(filetogen)==0:
                            logger_test.info("Keyreport file is not present so creating it ")
                            aggregator12()
                   else:
                            logger_test.info("Keyreport file is already present,so skipping creation")
                            pass 
                   logger_test.info("Data Download complete")                
                   self.previousdatadelete()
                   self.filepath=os.getcwd()
                   logger_test.info("Loading the data into table ")
                   self.loadquery="LOAD DATA  INFILE '%s/KeyReport.csv' INTO TABLE CS109.Keyreport CHARACTER SET UTF8 FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\n' IGNORE 1 ROWS "%self.filepath
                   logger_test.info("query fired for loading the data %s"%self.loadquery)
                   self.handler.execute(self.loadquery)
                   logger_test.info("loaded new data into the table")
       def processdata(self):
                 self.loadnewdata()
                 self.query='select "original_keyword","accountcount","campaigncount","cost" union all select Replace(Replace(Replace(concat(trim(Both "+" from  trim(Both "#" from Keyword)),"_",Match_type),"+",""),"]",""),"[",""),count(distinct(Account)),count(distinct(Campaign)),(sum(Cost)/1000000) from CS109.Keyreport group by Keyword,Match_type INTO OUTFILE "%s/perf.csv" CHARACTER SET UTF8 FIELDS TERMINATED BY "," ENCLOSED BY "\\"" LINES TERMINATED BY "\\n"'%self.filepath
                 logger_test.info("executing query %s"%self.query)
                 self.handler.execute(self.query)
                 logger_test.info("perf data created")
                 self.keywordquery='select "Original_keyword","Customer_ID","Account","Campaign","Campaign_state","Ad_group","Ad_group_state","Keyword","Keyword_state","Match_Type","clicks","impressions","cost" union all select Replace(Replace(Replace(concat(trim(Both "+" from  trim(Both "#" from Keyword)),"_",Match_type),"+",""),"]",""),"[","") as c1,Customer_ID,Account,Campaign,Campaign_state,Ad_group,Ad_group_state,Keyword,Keyword_state,Match_type,sum(Clicks),sum(Impressions),(sum(Cost)/1000000) from Keyreport group by c1,Customer_ID,Account,Campaign,Campaign_state,Ad_group,Ad_group_state,Keyword,Keyword_state,Match_type INTO OUTFILE "%s/key.csv" CHARACTER SET UTF8 FIELDS TERMINATED BY "," ENCLOSED BY "\\"" LINES TERMINATED BY "\\n"'%self.filepath   
                 logger_test.info("executing query %s"%self.keywordquery)
                 self.handler.execute(self.keywordquery)
                 logger_test.info("keyword data created")
                 self.query2="delete from table CS109.Keyreport"
                 logger_test.info("executing query %s"%self.query2)
                 self.handler.execute(self.query2)
                 logger_test.info("old data deleted")
                 
       def excelcreator(self):
                 self.processdata()
                 current_interim_date=datetime.date.today()
                 current_date=current_interim_date.isoformat()
                 logger_test.info("starting making excel file")
                 writer = pd.ExcelWriter('DATAbimonthly_%s.xlsx'%current_date,engine='xlsxwriter')
                 all_key=pd.read_csv("key.csv",encoding="utf-8")
                 all_perf=pd.read_csv("perf.csv",encoding="utf-8")
                 all_perf=all_perf.sort_values('cost',ascending=False)
                 all_key.to_excel(writer,"Keyword",index=False,encoding="utf-8")
                 all_perf.to_excel(writer,"Perf",index=False,encoding="utf-8")
                 #writer.close()
       def finalfilecreator(self):
               #os.system("rm *.csv")
               self.excelcreator()
if __name__=="__main__":
     
          t1=monthlydata()
          t1.finalfilecreator()
 
          
