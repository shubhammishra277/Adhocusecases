# Keyworddedup

This code is used to find the duplicate keywords and their corresponding cost with account count and campaign count running on Google adwords Platform.

<h1><b>Input/Ouput of the project</b></h1> 


<b>1.Input</b>:Input to the code is a google drive sheet.In that sheet ,user defines the account ids and account names corresponding to which h
               he wants the to check keyword duplication.
               
               
               
<b>2.Output</b>:Ouput of the code is automatically uploaded to the directory in the google drive.Output file is excel sheet containing 
                2 tabs.First tab contains all the heirrachial information i.e adgroup,adgroupid,campaign,campaignid,accountid,status etc. 
                of the keywords.Second tab is the summary tab containing 4 columns and description is as follows:
                1 column contains the keyword,2 column contains the number of accounts in which this keyword is present ,3 column contains
                the nummber of campaigns in which keyword appeared and the last column conatins the total or aggreagated cost for this keyword.
                Second tab of the output sheet is sorted on the basis of cost column 


<h1><b>Types of report generated</b></h1>:

<b>Bimonthly Report</b>:In this feature ,code runs automatically on a particular day of the month specified by user in accounts.config
                    .ini defined against TimeDefination property.Timedefination property also contains the number of days for which the 
                    keyword report should be downloaded.
                    
                    
<b>Daily report</b>:In this report,code runs on daily basis .A set of keyword list for which we want to check whether they are already running on google adwords  
                is given as input to the code.Code runs gives the keywords and their corresponding cost with account count and campaign count 
                    
                    
                    

