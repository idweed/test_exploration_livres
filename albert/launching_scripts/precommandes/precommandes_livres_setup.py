#!/usr/bin/env python3
import datetime


# today's date - AAAA-MM-DD
today_ymd = datetime.datetime.now().strftime('%Y-%m-%d')

# today's date - DD/MM/AAAA
today_dmy = datetime.datetime.now().strftime('%d/%m/%Y')

# spider timeout in minutes
spider_timeout = 5

# list of spiders to launch
spider_names = ['precommandes_amazon_livres', 'precommandes_fnac_livres']

# project id in scrapycloud
project_id = 436828

# mail subject
subject = 'Crawl Précommandes Livres'

# file to send
attached_path = '/tmp/'

# filename for final data
filename = f'precommandes_livres_{today_ymd}.csv'


# mail recipients
def mail_recipients(env):
    if env == 'DEV':
        return ['sophie.nassour-ext@fnacdarty.com']
    else:
        return ['sophie.nassour-ext@fnacdarty.com']


# cc mail recipients
def cc_mail_recipients(env):
    if env == 'DEV':
        return ['sophie.nassour-ext@fnacdarty.com']
    else:
        return ['sophie.nassour-ext@fnacdarty.com']


# email message
def message_text(today_dmy, fnac_deposit_upload='OK'):
    return f'<p>Bonjour,\n \
    <br /><br />Le résultat du {subject} du {today_dmy} est à votre disposition en pièce(s) jointe(s).\
    <br /><br />Upload sur Fnac Deposit : {fnac_deposit_upload}.</p>\
    <br /><br />— Ceci est un message envoyé automatiquement.</p>'
