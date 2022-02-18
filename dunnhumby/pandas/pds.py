import pandas as pd
pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', -1)
"""
Email Sending Functionality for Failure Notification
"""
#############################
import traceback
import smtplib
import mimetypes
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.message import Message
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
import datetime
import threading


def send_mail(config):
    print('send_mail: started')

    # getting the current date
    cu_date = datetime.datetime.now().date()

    receivers_list = "myeranow2020@gmail.com"
    to_list = "madhuchaitanya.joshi@gmail.com"
    cc_list = "joshimadhuchaitanya@gmail.com"
    #module_name = config['email_param']['module_name']
    sender = "myeranow2020@gmail.com"

    body = "<p>Hello Team,</p>" + \
           "<p>This is to notify that job has <strong>failed</strong> for the below mentioned date. Please look into the logs for further verification.</p>" + \
           "<p><strong><span style=\"text-decoration: underline;\">DATE :" + str(
        cu_date) + "</span>&nbsp;&nbsp;</strong></p>" + \
           "<p>&nbsp;</p>" + \
           "<p><strong>Thanks</strong></p>" + \
           "<p><strong>Measurement Notifications</strong></p>"
    print(type(body))
    for p in config:
        print(p)
        body = body+"<p>{} {}</p>".format(config[p]["eid"], config[p]["email"])

    message = MIMEMultipart("alternative")
    message['Subject'] ="Hello World madhu"
    message['From'] = sender
    message['To'] = to_list
    message['CC'] = cc_list
    message.attach(MIMEText(body, 'HTML'))

    #filename = f'omm_kpi_{str(cu_date.day).rjust(2, "0")}_{str(cu_date.month).rjust(2, "0")}.log'
    #     filename = 'omm_kpi_14_05.log'
    part = MIMEBase('application', "octet-stream")
    open_file = 0

    # while open_file == 0:
    #     part.set_payload(open(filename, "rb").read())
    #     encoders.encode_base64(part)
    #     part.add_header('Content-Disposition', 'attachment', filename=filename)
    #     message.attach(part)
    #     open_file = 1

    mail_sender(
        receivers=receivers_list,
        sender=sender,
        message=message.as_string())

    print('send_mail: Ends')
    return


def mail_sender(receivers, sender, message):
    server = smtplib.SMTP('smtp.gmail.com', 587)
    server.set_debuglevel(1)
    server.starttls()
    server.ehlo()
    server.login(sender, "Communistcomrade")

    #server.sendmail(sender, receivers, message)
    server.quit()
    return


def send_mail_async(config):
    email_thread = threading.Thread(target=send_mail, args=(config,))
    email_thread.start()
    return

# my_df  = pd.DataFrame()
#print(my_df)
#iris = pd.read_csv("C:/data/emp.csv")
iris = pd.read_csv("C:/data/dunnhumbyDataMail.csv")
print(iris.to_string())

iris = iris.drop_duplicates()
#k = iris.groupby('campaign_id')[].apply(dict)
g=['status_desc','mailid']
k = iris.groupby(['campaign_id','status','report_type','campaign_name','date'],
                  as_index=False)[g].agg(lambda x: list(set(x)))
print(k)
k2 = k.set_index(['campaign_id','status','report_type','campaign_name']).T.to_dict('dict')
print(k2)
#k = iris.set_index(['campaign_id','report_type']).T.to_dict('dict')
# print(k)

#send_mail_async(k)

#print("-----------------",my_df,len(my_df.index))
#my_df = my_df.append(iris)
#print("----------------",my_df,len(my_df.index))