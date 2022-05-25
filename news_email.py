import smtplib, ssl

port = 465  # For SSL

#setup env variable with password
password = 'password'
sender_email = 'ornellodev2@gmail.com'
receiver_email = 'iannaceornello@gmail.com'
message = '''
Hello sir,

Goodbye sir.
'''

# Create a secure SSL context
context = ssl.create_default_context()

with smtplib.SMTP_SSL("smtp.gmail.com", port, context=context) as server:
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email, message)

