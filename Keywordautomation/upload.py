import datetime
from string import ascii_uppercase
import os
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
import sys
import xlsxwriter
import re
import time
import glob
from loggermodule import logger_test


def idgetter():
  gauth = GoogleAuth()
  gauth.LoadCredentialsFile("credentials.json")
  if gauth.credentials is None:
    gauth.LocalWebserverAuth()
  elif gauth.access_token_expired:
    gauth.Refresh()
  else:
    gauth.Authorize()
  gauth.SaveCredentialsFile("credentials.json")

  drive = GoogleDrive(gauth)
  current_interim_date=datetime.date.today()- datetime.timedelta(1)
  current_date=current_interim_date.isoformat()
  file_list = drive.ListFile({'q': "'root' in parents and trashed=false"}).GetList()
  for file1 in file_list:
    if file1['title'] == "Keyword Dedup Checker":
        id = file1['id']
        break
  file_list_child=drive.ListFile({'q': "'%s' in parents and trashed=false"%id}).GetList()
  for file12 in file_list_child:
      if file12['title']=="Output Files":
           inter_id_upload_daily=file12['id']
      elif file12['title']=="Input Files":
             inter_id_input=file12['id']
      elif file12['title']=="Automated reports":
             inter_id_bimonthly=file12['id']
             #print id_bimonthly
      elif file12['title']=="Processed Files":
             inter_id_mover=file12['id']

  return drive,inter_id_upload_daily,inter_id_input,inter_id_mover,inter_id_bimonthly

  
def bimonthlyuploader(drive,id_bimonthly,current_date):
 try:
  f = drive.CreateFile({"parents": [{"kind": "drive#fileLink", "id": id_bimonthly}]})
  f.SetContentFile("DATAbimonthly_%s.xlsx"%current_date)
  f.Upload()
  
  logger_test.info("Bimonthly uploaded for date %s"%current_date)
 except:
   logger_test.exception("Bimontly upload failed for date %s"%current_date)

def dailyuploader(drive,id_upload_daily,current_date):
     filelist=[i for i in glob.glob("IBM_*.xlsx")]
     mailinglist=[]
     for files in filelist:
        try:
            f = drive.CreateFile({"parents": [{"kind": "drive#fileLink", "id":id_upload_daily }]})
            f.SetContentFile(files)
            f.Upload()
            mailinglist.append(files)
            logger_test.info("Daily file named %s uploaded for date %s"%(files,current_date))
        except:
            logger_test.exception("Daily file named %s  upload failed for date %s"%(files,current_date))
            continue
     filefailed=list(set(filelist)-set(mailinglist))
     return filefailed

def downloader(drive,id_input,current_date):
    
    file_list_child_input=drive.ListFile({'q': "'%s' in parents and trashed=false"%id_input}).GetList()
    file_ids={}
    mailinglist=[] 
    for i in file_list_child_input:
          file_ids[i['id']]=i['title']

    for y in file_ids:
         try:
             file_obj = drive.CreateFile({'id':y})
             file_obj.GetContentFile(file_ids[y])
             mailinglist.append(y)
             logger_test.info("file named %s downloaded successfully"%file_ids[y])
         except:
             logger_test.exception("file named %s download failed"%file_ids[y])
             continue
    filefailed=list(set(file_ids)-set(mailinglist))
    return filefailed
def mover(drive,id_input,id_mover):
    file_list_child_input=drive.ListFile({'q': "'%s' in parents and trashed=false"%id_input}).GetList()
    for file in file_list_child_input:
                  file_obj = drive.CreateFile({'id':file['id']}) 
                  file_obj.Delete()
    filelist=[i for i in glob.glob("IBM_*.csv")]
    mailinglist=[]
    for i in filelist:
          try:
            f = drive.CreateFile({"parents": [{"kind": "drive#fileLink", "id":id_mover}]})
            f.SetContentFile(i)
            f.Upload()
            mailinglist.append(i)
            logger_test.info("Daily file named %s moved to processed folder"%i)
          except:
            logger_test.exception("Daily file named %s  upload failed for date %s"%i)
            continue
    filefailed=list(set(filelist)-set(mailinglist))
    return filefailed

        
if __name__=="__main__":

          current_interim_date=datetime.date.today()- datetime.timedelta(1)
          current_date=current_interim_date.isoformat()
     
          drive,inter_id_upload_daily,inter_id_input,inter_id_mover,inter_id_bimonthly=idgetter()
          #bimonthlyuploader(drive,inter_id_bimonthly,current_date)
          #download_mover(drive,inter_id_input,current_date)
          #mover(drive,inter_id_input,inter_id_mover,current_date)
          k=mover(drive,inter_id_input,inter_id_mover)
