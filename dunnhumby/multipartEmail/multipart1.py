import smtplib, ssl
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

sender_email = "myeranow2020@gmail.com"
receiver_email = "madhuchaitanya.joshi@gmail.com"
password = "Communistcomrade"

message = MIMEMultipart("mixed")
message["Subject"] = "multipart test"
message["From"] = sender_email
message["To"] = receiver_email
print("\n\n\n",message)
# Create the plain-text and HTML version of your message
text = """\
Hi text,
How www.are.com you?
Real Python has many great tutorials:
www.realpython.com"""
html = """\
<html>
  <body>
    <p>Hi html,<br>
       How are you?<br>
       <a href="http://www.realpython.com">Real Python</a> 
       has many great tutorials.
    </p>
  </body>
</html>
"""

# Turn these into plain/html MIMEText objects
part2 = MIMEText(html, "html")
part1 = MIMEText(text, "plain")
print(part1,part2)
# Add HTML/plain-text parts to MIMEMultipart message
# The email client will try to render the last part first
message.attach(part2)
message.attach(part1)

# Create secure connection with server and send email
context = ssl.create_default_context()
with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
    server.login(sender_email, password)
    server.sendmail(
        sender_email, receiver_email, message.as_string()
    )