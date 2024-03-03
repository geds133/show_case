import smtplib
from email.mime.text import MIMEText

password = "Slowbro99!"

from email.message import EmailMessage
import smtplib

sender = "gerardchurch133@outlook.com"
recipient = "gerardchurch133@outlook.com"
message = "Hello world!"

email = EmailMessage()
email["From"] = sender
email["To"] = recipient
email["Subject"] = "Test Email"
email.set_content(message)

smtp = smtplib.SMTP("smtp-mail.outlook.com", port=587)
smtp.starttls()
smtp.login(sender, password)
smtp.sendmail(sender, recipient, email.as_string())
smtp.quit()