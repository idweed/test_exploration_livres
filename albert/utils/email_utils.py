import email.mime.application
import email.mime.multipart
import email.mime.text
import smtplib
import logging


def send_data(mail_recipients, cc_mail_recipients, subject, attachment_path, filename, message_text, sender):
    message = email.mime.multipart.MIMEMultipart()
    message['From'] = sender.username
    message['To'] = ', '.join(mail_recipients)
    message['CC'] = ', '.join(cc_mail_recipients)
    message['Subject'] = subject
    message.attach(email.mime.text.MIMEText(message_text, 'html', 'utf_8'))
    try:
        smtp = smtplib.SMTP(host=sender.host, timeout=60)
    except Exception as e:
        logging.error('Failed to establish an SMTP connection to remote host: {}\nreason : {}'.format(sender.host, e))
        return
    logging.info('Established an SMTP connection to remote host: {}'.format(sender.host))
    try:
        smtp.starttls()
    except (smtplib.SMTPHeloError, smtplib.SMTPException, RuntimeError):
        logging.error('Failed to put the SMTP connection in TLS mode')
        return
    logging.info('Put the SMTP connection in TLS mode')
    try:
        smtp.login(sender.username, sender.password)
    except (smtplib.SMTPHeloError, smtplib.SMTPAuthenticationError, smtplib.SMTPException):
        logging.error('Failed to log in on remote host')
        return
    logging.info('Logged in on remote host')
    try:
        f = open(attachment_path, "rb")
    except Exception as e:
        logging.error('Failed to open/read file: {}\nreason : {}'.format(attachment_path, e))
        return
    with f:
        attachment = email.mime.application.MIMEApplication(f.read())
        attachment.add_header('Content-Disposition', 'attachment', filename=filename)
        message.attach(attachment)
    try:
        smtp.sendmail(sender.username, mail_recipients + cc_mail_recipients, message.as_string())
    except (smtplib.SMTPRecipientsRefused, smtplib.SMTPHeloError, smtplib.SMTPSenderRefused, smtplib.SMTPDataError):
        logging.error('Failed to send email')
        return
    logging.info(
        'Sent an email: to_recipients = [' + message['To'] + ']; cc_recipients = [' + message['Cc'] + ']' + (
            '; attached files = ' + ', '.join(attachment_path) if isinstance(attachment_path, list) else ''))
    smtp.quit()
    logging.info('Closed connection to remote host')
