from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import subprocess
import time
msg = MIMEMultipart()
#frome = "mygmail@gmail.com"
frome = "mygmail@gmail.com"
frompass = "password"
toe = ["mygmail@gmail.com"]

processes = {}
def emailSend(host,port,frome,toe,body,subject):
    inp = 'Subject: {}\n\n{}'.format(subject, body)
    with smtplib.SMTP(host=host, port=port) as smtp_obj:
        smtp_obj.ehlo()
        smtp_obj.starttls()
        smtp_obj.ehlo()
        smtp_obj.login(frome, frompass)
        smtp_obj.sendmail(frome, toe, inp)

def Poller():
    for child,value in processes.copy().items():
        child.poll()
        print("--------->",child.returncode,"\t\t",processes)
        if(child.returncode is not None and child.returncode!=0):
                print(value)
                print("inside OneCond")
                value.insert(4,child.stderr.read())
                value.pop(5)
                value.insert(5,"The script ends in failure")
                value.pop(6)
                emailSend(value[0],value[1],value[2],value[3],value[4],value[5])
                del processes[child]
        elif(child.returncode==0):
            print("inside condTwo")
            print("script completes here")
            subject = "script ran successfully"
            body = "script ran successfully"
            emailSend("smtp.gmail.com", 587, frome, toe, body, subject)
            del processes[child]



subject =  "script started email subject"
body= "messsae starts here itself"
emailSend("smtp.gmail.com",587,frome,toe,subject,body)
command="spark-submit C:/Users/Madhu/PycharmProjects/SparkProject/rdd1.py"
process = subprocess.Popen(command, shell=True, stdin=None, stdout=None, stderr=subprocess.PIPE, close_fds=True,universal_newlines=True)
pid = process.pid
processes[process] = ["smtp.gmail.com",587,frome,toe,subject,body]

while len(processes)>0:
    Poller()
    print("+++++++++++++++++++++++++++",processes)