import os
import sys
from loggermodule import logger_test
import DriveFunctions as DF
from ConfigParser import RawConfigParser

def CreateClientDictionary():
       interim_dict=configparse()
       text=interim_dict.values().pop()
 
       text=text.split(":{")
       testtext="{"+text[1]
       testtext=eval(testtext)
       dictionary={}
     
       dictionary[text[0]]=testtext
       logger_test.info("url of sheet which we are hitting %s"%dictionary)
       return dictionary
def configparse():

             parser = RawConfigParser()
             parser.read('accounts_config.ini')
             account_details={}
             for section_name in parser.sections():
                   k=parser.items(section_name)
                   adder_part=":{'NOMBRE':'%s'}"%section_name
                   url=str(k[0][1])+adder_part
                   client_name=section_name
                   account_details[client_name]=url
             return account_details
def client():
     ClientDictionary = CreateClientDictionary()    
     logger_test.info("URL from which we are getting list of accounts:%s"%ClientDictionary) 
     diclient_temp = {}
     for client in ClientDictionary:
        gc = DF.ConnectToDrive()
        logger_test.info("Connected to google drive\n")
        libro = gc.open_by_url(client)
        sheet=libro.worksheet(ClientDictionary[client]['NOMBRE'])
        diclient_temp= sheet.get_all_records()
        Destino_temp = client
     return Destino_temp,diclient_temp 
def creator():
      current_dir=os.getcwd()
      parser = RawConfigParser()
      parser.read('accounts_config.ini')
      client_name=parser.sections().pop()
      v,k=client()
      k=[i['ACCOUNT_ID'] for i in k]
      m=parser.items(client_name)
      
      os.system("mkdir -p YamlFiles")
      #os.chdir("YamlFiles/")
      for i in k:
          f=open("YamlFiles/googleads_%s.yaml"%i,'w+')
          f.write("adwords:\n")
          f.write("  developer_token: %s\n"%m[1][1])
          f.write("  user_agent: %s\n"%m[2][1])
          f.write("  client_id: %s\n"%m[3][1])
          f.write("  client_secret: %s\n"%m[4][1])
          f.write("  refresh_token: %s\n"%m[5][1])
          f.write("  client_customer_id: %s"%i)
          f.close()

if __name__=="__main__":
      creator()
