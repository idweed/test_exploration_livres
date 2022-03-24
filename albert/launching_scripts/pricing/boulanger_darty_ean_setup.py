#!/usr/bin/env python3
import albert.config.config as config


if config.env == 'DEV':
    mail_recipients = ['sophie.nassour-ext@fnacdarty.com']
    cc_mail_recipients = ['sophie.nassour-ext@fnacdarty.com']
else:
    mail_recipients = ['lydia.pigani@fnacdarty.com']
    cc_mail_recipients = ['sophie.nassour-ext@fnacdarty.com']

project_id = 436828
spider_names = ['boulanger_ean', 'darty_ean']
subject = 'Crawl Boulanger - Darty PEM'
final_filename = 'boulanger_darty_PEM_'


def message_text(today_dmy):
    return f'<p>Bonjour,\n \
    <br /><br />Le résultat du crawl Boulanger - Darty PEM du {today_dmy} est à votre disposition en pièce(s) jointe(s).\
    <br /><br />— Ceci est un message envoyé automatiquement.</p>'
