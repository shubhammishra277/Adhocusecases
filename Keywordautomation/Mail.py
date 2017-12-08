import mandrill
from ConfigParser import SafeConfigParser

class mail(object):
    def __init__(self):
         self.MANDRILL_API_KEY = 'JJgvzSiBFGGoe-9v6eEoHw'
         self.mandrill_client = mandrill.Mandrill(self.MANDRILL_API_KEY)
         self.message = { 'from_email': 'neo_support@ogilvy.com',
         'from_name': 'SUPPORT',
         'to': [{
         'email': 'shubham.mishra@ogilvy.com',
         'name': 'shubham',
          'type': 'to'
        }],
        'subject': "Keyword Automation Job Status",
        'html': 'This is a message from Mandrill'
        }
    def upload_job_fail(self,current,Value):
       self.message['subject']="Keyword automation file upload Failed"
       print self.extracter()
       self.message['to']=self.extracter()
       #self.message['subject']="UPLOAD JOB FAILED FOR DATE %s"%current
      
       self.message['html']='''<h3>Status:</h3>
                             <p>%s for date %s has failed</p>
                             <h3>Action</h3>
                             <p>Please contact support team at neo_support@ogilvy.com</p>
                             <h4>Thanks and Regards</h4>
                             <p>Support Team</p>'''%(Value,current)
                    
                              
        #self.message['text']="Upload Job has failed for date %s due to connectivity mail.\n\nThanks and Regards,\nSupport"%current 
       self.send()
    def jobchecker_completed(self,current,Value):
        self.message['subject']="Keyword automation Job completed"
        self.message['to']=self.extracter()
        self.message['html']='''<h3>Status:</h3>
                             <p>%s for date %s completed successfully</p>
                             <h3>Action</h4>
                             <p>No action required</p>
                             <p></p>
                             <p></p>
                             <h4>Thanks and Regards</h4>
                             <p>Support Team</p>'''%(Value,current)
        #self.message['subject']="URLHEALTH JOB STATUS FOR DATE %s"%current
        #self.message['text']="URL health job was luanched for this date %s successfully.\n\nThanks and Regards,\nSupport"%current
                
        self.send()
    def jobchecker_started(self,current,Value):
        self.message['subject']="Keyword automation  Job started"
        self.message['to']=self.extracter()
        self.message['html']='''<h3>Status:</h3>
                             <p>%s job for date %s  started successfully</p>
                             <h3>Action</h4>
                             <p>No action required</p>
                             <p></p>
                             <p></p>
                             <h4>Thanks and Regards</h4>
                             <p>Support Team</p>'''%(Value,current)
        #self.message['subject']="URLHEALTH JOB STATUS FOR DATE %s"%current
        #self.message['text']="URL health job was luanched for this date %s successfully.\n\nThanks and Regards,\nSupport"%current
                
        self.send()

    def no_file_fail(self,current,Value):
       self.message['subject']="Keyword automation file upload failed"
       print self.extracter()
       self.message['to']=self.extracter()
       #self.message['subject']="UPLOAD JOB FAILED FOR DATE %s"%current

       self.message['html']='''<h3>Status:</h3>
                             <p>%s for date %s has failed as no input files were present in input directory</p>
                             <h3>Action</h3>
                             <p>Please contact support team at neo_support@ogilvy.com</p>
                             <h4>Thanks and Regards</h4>
                             <p>Support Team</p>'''%(Value,current)


        #self.message['text']="Upload Job has failed for date %s due to connectivity mail.\n\nThanks and Regards,\nSupport"%current 
       self.send()




    def extracter(self):
        self.parser = SafeConfigParser()
        self.parser.read('config.ini')
        #print self.parser.sections()
        all_recipients=[]
        for section_name in self.parser.sections():
                   k=self.parser.items(section_name)
               
                   d=dict()
                   name=k[0][1]
                   value=k[1][1]
                   d['email']=name
                   d['name']=value
                   d['type']='to'
                   all_recipients.append(d)
        return all_recipients

    def send(self):
          print "inside send"
          result =self.mandrill_client.messages.send(message = self.message)
          #print result
if __name__=="__main__":
    t1=mail()
    #t1.send()
    val=0
    current="2016-11-05"
    t1.jobchecker_started(current)
    t1.upload_job_fail(val, current)
    t1.jobchecker_completed(current)
