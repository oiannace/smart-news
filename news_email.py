import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import date
port = 465  # For SSL

#setup env variable with password
password = 'password'
sender_email = 'ornellodev2@gmail.com'
receiver_email = 'iannaceornello@gmail.com'
message = MIMEMultipart("alternative")
message["Subject"] = 'Twitter Smart Update ' + str(date.today())
message["From"] = sender_email
message["To"] = receiver_email


html = """\
<html>
  <body>
    <p>Hi,<br>
       This is news!br?
    </p>
  </body>
</html>
"""

html1 = MIMEText(html, "html")
message.attach(html1)
# Create a secure SSL context
context = ssl.create_default_context()

with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email, message.as_string())

