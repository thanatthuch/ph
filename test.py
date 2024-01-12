import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Email configuration
smtp_server = 'localhost'
smtp_port = 587  # This is the default port for TLS
smtp_username = 'thanatthuch.cu@gmail.com'
smtp_password = 'Spy@May090922'
sender_email = 'thanatthuch.cu@gmail.com'
receiver_email = 'thanatthuchkamseng@gmail.com'

# Create a message object
message = MIMEMultipart()
message['From'] = sender_email
message['To'] = receiver_email
message['Subject'] = 'Test Email'

# Email body
body = 'This is a test email sent from Python.'
message.attach(MIMEText(body, 'plain'))

# Create a secure SSL/TLS connection with the SMTP server
with smtplib.SMTP(smtp_server, smtp_port) as server:
    server.starttls()
    server.login(smtp_username, smtp_password)
    text = message.as_string()
    server.sendmail(sender_email, receiver_email, text)

print('Email sent successfully!')
