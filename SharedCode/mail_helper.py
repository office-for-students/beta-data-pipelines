import os
import logging

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, Personalization, Content


class MailHelper:
    def __init__(self):
        self.enabled = True if os.environ.get("SendGridEnabled", "") == "True" else False
        self.send_grid = SendGridAPIClient(os.environ["SendGridAPIKey"])
        self.from_email = (os.environ["SendGridFromEmail"], os.environ["SendGridFromName"])
        self.to_emails = os.environ["SendGridToEmailList"].split(";")

    def send_message(self, message, subject):
        if not self.enabled:
            return

        mail = Mail(from_email=self.from_email, to_emails=self.to_emails, subject=subject, plain_text_content=message)

        try:
            response = self.send_grid.send(mail)
            return response
        except Exception as e:
            raise e
        