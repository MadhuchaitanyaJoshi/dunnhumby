from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib

msg = MIMEMultipart()
#frome = "myeranow2020@gmail.com"
frome = "madhuchaitanya.joshi@xoriant.com"
frompas = "Einstein@123"
toe = ["myeranow2020@gmail.com","madhuchaitanya.joshi@gmail.com","madhuchaitanya.joshi@xoriant.com"]
subject =  "script started email subject"
message = "script started message"
total = 'Subject: {}\n\n{}'.format(subject, message)
with smtplib.SMTP_SSL(host="smtp.xoriant.com", port=25) as smtp_obj:
    smtp_obj.ehlo()
    smtp_obj.ehlo()
    smtp_obj.login(frome,frompas)
    smtp_obj.sendmail(frome,toe,total)

print("¡Datos enviados con éxito!")

