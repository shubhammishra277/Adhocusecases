import pandas as pd 
from keywords_bak import aggregator12,parse
import multiprocessing
import os
import glob
import time
import datetime 
from ConfigParser import RawConfigParser
from loggermodule import logger_test
from fast import monthlydata
import upload
import io 
import sys 
import sys 
from Mail import mail
reload(sys)  
sys.setdefaultencoding('utf8')
 
def keywordparser(filepath2):
   try:
    path=os.getcwd()
    timevalue,filepath,defination_value=parse()
    f=io.open("%s"%filepath2,"r",encoding="utf8")
    interimlist=f.readlines()
    finallist=[i.strip("\n").strip("\r").replace("+","").replace('"',"").strip().strip("]").strip("[").decode("utf-8") for i in interimlist if "#" not in i ]
   
           
   except IOError:
    logger_test.exception("Keyword file is not present")
    #sys.exit()
    pass
   return finallist



def partitionor(filepath,function,str_function):
        print "filepath for calculation",filepath
        URLdf=function(filepath)
        partition_length=len(URLdf)/14
        interim_data=[]
        URLdf=list(URLdf)
        initial=0
      
        for i in range(14):
            interim_df=[]
            interim_df=URLdf[initial:(initial+partition_length)]
            initial=initial+partition_length
            interim_data.append(interim_df)
       
        interim_df=URLdf[initial:]
        interim_data.append(interim_df)
        # interim_data
        return interim_data
def process_creator(filepath,function,str_function):
    k=pd.read_csv("KeyReport.csv")
    inter_val=partitionor(filepath,function,str_function)
    process=["p"+str(i) for i in range(15)]
    for y in range(15):

        process[y]=multiprocessing.Process(target=calculator, args=(inter_val[y],k,y,filepath,function,str_function,))

    for y in process:
           y.start()

    while True:
      time.sleep(5)
      if not multiprocessing.active_children():
        break
def getterdaily(filepath):
  #path=os.getcwd()
  m=keywordparser(filepath)
  m=set(m)
  logger_test.info("No of unique keywors for daily report %s"%len(m))
  return m

def calculator(m,interdf,y,path,function,str_function):
 logger_test.info("process no. %s started "%y)

 all_data=pd.DataFrame()
 if len(m)==0:
   all_data=pd.DataFrame(columns=["Original_Keyword","Customer ID","Account","Campaign","Campaign state","Ad group","Ad group state","Keyword","Keyword state","Match type","Clicks","Impressions","Cost"])
 else:  
 
  for i in m:
   try:
      interim_data=interdf[interdf.Keyword.apply(lambda x :x.replace("+","").replace('"',"").strip().strip("]").strip("[").lower())=="%s"%i.encode("utf-8").lower()]
      interim_data["Cost"]=interim_data["Cost"].apply(lambda x:float(float(x)/(10**6)))

      interim_data["Original_Keyword"]=i
 
      colkey=["Original_Keyword","Customer ID","Account","Campaign","Campaign state","Ad group","Ad group state","Keyword","Keyword state","Match type","Clicks","Impressions","Cost"]
      interim_data2=interim_data.reindex(columns=colkey)
      all_data=all_data.append(interim_data2,ignore_index=True)
    
   except ValueError as e:
       
        logger_test.exception("error occured for ",str(e))
        continue 

 if str_function=="getterdaily":
   logger_test.info("inside daily loop")
   writer2=sheetcreator("Daily",y)
   second=perfreporter(all_data) 
   all_data.to_excel(writer2,"Keyword",index=False,encoding="utf-8")
   second.to_excel(writer2,"Performance",index=False,encoding="utf-8")
   #writer2.save()
  
 logger_test.info("Data file for %s partition is created"%y)
 logger_test.info("This process %s is complete"%y)
def sheetcreator(timecounter,count):
     writer = pd.ExcelWriter('DATA%s_%s.xlsx'%(timecounter,count),engine='xlsxwriter') 
     return writer
def perfreporter(all_data):
    t1=all_data["Original_Keyword"]
    perf=pd.DataFrame()
    t1=set(t1)
    col=["Keyword","AccountCount","CampaignCount","Cost"]
    keyword=[]
    AccountCount=[]
    CampaignCount=[]
    Cost=[]
    for i in t1:
        logger_test.info("starting for %s keyword"%i)
        cost=all_data.groupby("Original_Keyword")["Cost"].sum()[i]
        interim_acount_count=all_data[all_data["Original_Keyword"]==i]["Account"]  
        account_count=len(set(interim_acount_count))
        interim_campaign_count=all_data[all_data["Original_Keyword"]==i]["Campaign"]  
        campaign_count=len(set(interim_campaign_count))
   
        keyword.append(i)
        AccountCount.append(account_count)
        CampaignCount.append(campaign_count)
        Cost.append(cost)
      
        logger_test.info("keyword,account_count,account_count,cost are %s,%s,%s,%s"%(i,account_count,campaign_count,cost))
       
    perf["Keyword"]=pd.Series(keyword)   
    perf["AccountCount"]=pd.Series(AccountCount)
    perf["CampaignCount"]=pd.Series(CampaignCount)
    perf["Cost"]=pd.Series(Cost)
    return perf
def commonfunction(Finalreport):
     aggregate_data=pd.DataFrame()
     perf_data=pd.DataFrame()
     for y in Finalreport:
        # y
        xls = pd.ExcelFile(y)
        for p in range(2):
            if p==0:
               s1=xls.parse(p)
               aggregate_data=aggregate_data.append(s1,ignore_index=True)

            if p==1:
               s2=xls.parse(p)
               perf_data=perf_data.append(s2,ignore_index=True)
     return aggregate_data,perf_data
def aggregator(filepath,function,str_function):
   
   present_file=[file for file in glob.glob("KeyReport.csv")]
   if len(present_file)==0:
       logger_test.info("Keyreport file not present,so creating it")
       aggregator12()
   logger_test.info("Keyreport file is present,so skipping creation")
   logger_test.info("calling process creator")
   process_creator(filepath,function,str_function)
   logger_test.info("Starting aggregation")
   filename=filepath.split(".")[0]
   if str_function=="getterdaily":
      
       Finalreport=[i for i in glob.glob("DATADaily_*.xlsx")]
       writer = pd.ExcelWriter('%s.xlsx'%filename,engine='xlsxwriter')
   
       all_agrregatedata,all_perfdata= commonfunction(Finalreport)
       all_agrregatedata.to_excel(writer,"Keyword",index=False) 
       all_perfdata=all_perfdata.sort_values('Cost',ascending=False)

       all_perfdata.to_excel(writer,"Perf",index=False)
      # writer.save()       
       os.system("rm DATADaily_*.xlsx")

if __name__=="__main__":
    current_interim_date=datetime.date.today()
    current_date=current_interim_date.isoformat()
    t2=mail()
    drive,inter_id_upload_daily,inter_id_input,inter_id_mover,inter_id_bimonthly=upload.idgetter()
    current_day=datetime.datetime.now().day
    timevalue,valuefilling,defination=parse()
    logger_test.info("Clearing previous csv and xlsx files")
    os.system("rm *.csv")
    os.system("rm *.xlsx")
    if (current_day==defination[timevalue][0]) or (current_day==defination[timevalue][1]):
        
          t2.jobchecker_started(current_date,"Bimonthly")
          logger_test.info("starting for by monthly report")
          t1=monthlydata()
          t1.finalfilecreator()
          try:
             upload.bimonthlyuploader(drive,inter_id_bimonthly,current_date)
          except:
             t2.upload_job_fail(current_date,"Bimonthly")
          t2.jobchecker_completed(current_date,"Bimonthly")    
          logger_test.info("starting for by daily report")
          t2.jobchecker_started(current_date,"Daily")
          
          downloadfile=upload.downloader(drive,inter_id_input,current_date)
          if len(downloadfile)!=0:
              logger_test.exception("Some files are not downloaded")
          filelist=[i for i in glob.glob("*.csv")]
          print "filelist",filelist

          logger_test.info("file list for processing %s"%filelist)
          if len(filelist) !=0:
             for file in filelist:
               if file not in ['perf.csv','KeyReport.csv', 'key.csv']:
                print "file for aggregator",file 
                aggregator(file,getterdaily,"getterdaily")
               
          else:
             logger_test.info("no input files are present for the day")
             t2.upload_job_fail(current_date,"Daily")
             sys.exit()
          uploadfile=upload.dailyuploader(drive,inter_id_upload_daily,current_date)
          if len(uploadfile)!=0:
                 logger_test.info("All files are not uploaded %s"%uploadfile)
                 t2.upload_job_fail(current_date,"Daily")

          moverfile=upload.mover(drive,inter_id_input,inter_id_mover)
          if len(moverfile)!=0:

                 logger_test.info("All files are not moved %s"%moverfile)
                 t2.upload_job_fail(current_date,"Daily")

          else:
              logger_test.info("Job completed")
              t2.jobchecker_completed(current_date,"Daily")
            
          os.system("rm *.csv")
    else:
          logger_test.info("starting for by daily report")
          t2.jobchecker_started(current_date,"Daily")

          downloadfile=upload.downloader(drive,inter_id_input,current_date)
          if len(downloadfile)!=0:
              logger_test.exception("Some files are not downloaded")
       
          filelist=[i for i in glob.glob("*.csv")]
          print "filelist",filelist

          logger_test.info("file list for processing %s"%filelist)

          if len(filelist) !=0:
             for file in filelist:
              if file not in ['perf.csv','KeyReport.csv','key.csv']:
               print "file for aggregator function",file
               aggregator(file,getterdaily,"getterdaily")

          else:
             logger_test.info("no input files are present for the day")
             t2.no_file_fail(current_date,"Daily")
             sys.exit()
          uploadfile=upload.dailyuploader(drive,inter_id_upload_daily,current_date)
          if len(uploadfile)!=0:
                 logger_test.info("All files are not uploaded %s"%uploadfile)
                 t2.upload_job_fail(current_date,"Daily")
                 logger_test.info("Movement of files from input folder to processed folder aborted")
                 sys.exit(0)
          moverfile=upload.mover(drive,inter_id_input,inter_id_mover)
          if len(moverfile)!=0:

                 logger_test.info("All files are not moved %s"%moverfile)
                 #t2.upload_job_fail(current_date,"Daily")

          else:
              logger_test.info("Job completed")
              t2.jobchecker_completed(current_date,"Daily")

          os.system("rm *.csv")
 
