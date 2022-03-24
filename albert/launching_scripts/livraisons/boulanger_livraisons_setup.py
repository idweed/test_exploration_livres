#!/usr/bin/env python3
import albert.config.config as config


if config.env == 'DEV':
    mail_recipients = ['sophie.nassour-ext@fnacdarty.com']
    cc_mail_recipients = ['sophie.nassour-ext@fnacdarty.com']
else:
    mail_recipients = ['olivier.guerin@fnacdarty.com']
    cc_mail_recipients = ['guillaume.ampe@fnacdarty.com', 'sophie.nassour-ext@fnacdarty.com']

project_id = 436828
spider_name = 'boulanger_livraisons'
subject = 'Crawl Boulanger Livraisons'


def message_text(today_dmy):
    return f'<p>Bonjour,\n \
    <br /><br />Le résultat du crawl Boulanger Livraisons du {today_dmy} est à votre disposition en pièce(s) jointe(s).\
    <br /><br />— Ceci est un message envoyé automatiquement.</p>'
