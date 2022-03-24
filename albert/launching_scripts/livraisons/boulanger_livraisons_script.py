#!/usr/bin/env python3
import pytz
import datetime
import pandas as pd
import sys
from numpy import nan as npnan
from albert.utils.email_utils import send_data
from albert.config.config import smtp_access
import albert.config.ftp_wrapper as ftp
from albert.launching_scripts.script_base import BaseScrapinghubScript
from albert.launching_scripts.livraisons.boulanger_livraisons_setup import *


def treat_data(data, final_path):
    decoded_data = [{key.decode(): val.decode() if key.decode() != 'Date de livraison' or 'Indisponible' in val.decode() else datetime.datetime.strptime(val.decode(), '%d/%m/%Y') for key, val in d.items()} for d in data]
    df = pd.DataFrame(decoded_data)
    df = df.drop(['_type', '_key'], axis=1)
    df['Date de livraison'] = pd.to_datetime(df['Date de livraison'], errors='coerce')
    df = df.sort_values('Date de livraison').drop_duplicates(subset='Ville', keep='first')
    df['Date de livraison'] = df['Date de livraison'].dt.strftime('%d/%m/%Y')
    df = df.replace(npnan, 'Indisponible', regex=True)
    df.to_csv(final_path, index=False)


def main():
    if len(sys.argv) > 1:
        env = sys.argv[1]
    else:
        env = config.env
    print(env)
    project_script = BaseScrapinghubScript(project_name=spider_name)
    project_script.logger()
    project_script.connect_to_scrapinghub()
    paris = pytz.timezone('Europe/Paris')
    if env != 'DEV':
        project_script.run_scrapinghub_spider(project_id, spider_name, minutes=5)
    data = project_script.get_scrapinghub_job_data(project_script.job_key)
    today_ymd_h = datetime.datetime.now(tz=paris).strftime("%Y-%m-%d-%H")
    filename = f'{spider_name}_{today_ymd_h}.csv'.replace('-13.csv', '-14.csv')
    attached_fullpath = f'/tmp/{filename}'
    treat_data(data, attached_fullpath)
    ftp.upload(config.fnac_deposit_ftp_access, attached_fullpath)
    today_dmy = datetime.datetime.now(tz=paris).strftime("%d/%m/%Y")
    if env == 'prod':
        send_data(mail_recipients,
                  cc_mail_recipients,
                  subject,
                  attached_fullpath,
                  filename,
                  message_text(today_dmy),
                  smtp_access)
    else:
        send_data(['sophie.nassour-ext@fnacdarty.com'],
                  ['sophie.nassour-ext@fnacdarty.com'],
                  subject,
                  attached_fullpath,
                  filename,
                  message_text('DEV !!!'),
                  smtp_access)


if __name__ == '__main__':
    main()
