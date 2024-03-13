from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail


class MailService:
    def __init__(self, send_grid_api_key, from_email, to_emails, enabled=True):
        self.send_grid = SendGridAPIClient(send_grid_api_key)  # os.environ["SendGridAPIKey"])
        self.from_email = from_email  # (os.environ["SendGridFromEmail"], os.environ["SendGridFromName"])
        self.to_emails = to_emails  # os.environ["SendGridToEmailList"].split(";")
        self.enabled = enabled  # True if os.environ.get("SendGridEnabled", "") == "True" else False

    def send_message(self, message, subject):
        if not self.enabled:
            return

        mail = Mail(from_email=self.from_email, to_emails=self.to_emails, subject=subject, plain_text_content=message)

        try:
            response = self.send_grid.send(mail)
            return response
        except Exception as e:
            raise e
