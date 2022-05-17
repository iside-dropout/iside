import os
import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

bodymsg = """
Cara studentessa, caro studente,

Ti chiediamo di rispondere liberamente alle seguenti domande che ci
permetteranno di valutare il corso di laurea del nostro dipartimento in
Sapienza. Il questionario è totalmente anonimo ed è stato inviato ad un
campione casuale ma limitato di studenti. Ti preghiamo quindi di
mantenerlo riservato e non condividerlo con altri studenti.

Link al questionario: %s

Ti saremmo grati se tu potessi compilare il questionario entro
Mercoledì 18 maggio.
"""


def send_email(email, form_link):
    if "SMTP_SERVER" not in os.environ:
        raise Exception("missing SMTP configuration")

    try:
        mail_from = os.getenv("MAIL_FROM")
        smtp_server = os.getenv("SMTP_SERVER")
        smtp_user = os.getenv("SMTP_USER")
        smtp_pass = os.getenv("SMTP_PASS")
        smtp_port = int(os.getenv("SMTP_PORT")) if "SMTP_PORT" in os.environ else 465

        message = MIMEMultipart()
        message["From"] = mail_from
        message["To"] = email
        message["Subject"] = "Questionario anonimo di valutazione sul corso di laurea in Sapienza"
        message.attach(MIMEText(bodymsg.strip() % form_link, "plain"))

        if smtp_port == 465:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(smtp_server, 465, context=context) as server:
                server.login(smtp_user, smtp_pass)
                server.sendmail(message["From"], message["To"], message.as_string())
                server.close()
        else:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.login(smtp_user, smtp_pass)
                server.sendmail(message["From"], message["To"], message.as_string())
                server.close()
    except Exception as e:
        print(e)


