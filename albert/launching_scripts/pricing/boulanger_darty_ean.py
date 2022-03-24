#!/usr/bin/env python3
import pytz
import datetime
import pandas as pd
# import sys
# from numpy import nan as npnan
from albert.utils.email_utils import send_data
from albert.config.config import smtp_access
import albert.config.ftp_wrapper as ftp
from albert.launching_scripts.script_base import BaseScrapinghubScript
from albert.launching_scripts.pricing.boulanger_darty_ean_setup import *


def treat_data(data, final_path):
    print(data)
    darty = data['darty_ean']
    boulanger = data['boulanger_ean']
    darty_decoded = [{key.decode(): val.decode() if val else '' for key, val in d.items()} for d in darty]
    boulanger_decoded = [{key.decode(): val.decode() if val else '' for key, val in d.items()} for d in boulanger]
    darty_df = pd.DataFrame(darty_decoded)
    boulanger_df = pd.DataFrame(boulanger_decoded)
    darty_df.drop(['_type', '_key', 'URL'], axis=1, inplace=True)
    boulanger_df.drop(['_type', '_key', 'URL'], axis=1, inplace=True)
    darty_df.rename(columns={'PRIX': 'DARTY_TTC'}, inplace=True)
    boulanger_df.rename(columns={'PRIX': 'BOULANGER_TTC'}, inplace=True)
    df = darty_df.merge(boulanger_df, on='EAN', how='outer')
    df.to_csv(final_path, index=False)


def main():
    env = 'prod'
    print(env)
    project_script = BaseScrapinghubScript(project_name=spider_names)
    project_script.logger()
    project_script.connect_to_scrapinghub()
    paris = pytz.timezone('Europe/Paris')
    project_script.run_scrapinghub_spider_list(project_id, spider_names, minutes=30)
    job_keys = project_script.job_keys
    # job_keys = ['436828/16/3', '436828/14/14']
    print(job_keys)
    data = project_script.get_scrapinghub_jobs_data(job_keys, spider_names)
    today_ymd = datetime.datetime.now(tz=paris).strftime("%Y-%m-%d")
    filename = f'{final_filename}{today_ymd}.csv'
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
