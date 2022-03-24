import sys
import logging
from albert.launching_scripts.script_base import BaseScrapinghubScript
import albert.config.config as config
from albert.launching_scripts.precommandes.precommandes_livres_setup import *
# import albert.config.ftp_wrapper as ftp
from albert.utils.email_utils import send_data


def get_data():
    return True


def treat_data():
    pass


def upload_data():
    pass


def crawl_amazon():
    pass


def crawl_fnac():
    pass


def integrate():
    pass


def filter():
    pass


def main():
    if len(sys.argv) > 1:
        env = sys.argv[1]
    else:
        env = config.env
    print(env)
    project_script = BaseScrapinghubScript(project_name=spider_names)
    project_script.logger()
    logging.info('environment is {}'.format(env))
    project_script.connect_to_scrapinghub()
    if env != 'DEV':
        project_script.run_scrapinghub_spider_list(project_id, spider_names, minutes=spider_timeout)
    data = get_data()
    treat_data()
    data_uploaded = upload_data()
    send_data(mail_recipients(env),
              cc_mail_recipients(env),
              subject,
              attached_path,
              filename,
              message_text(today_dmy, data_uploaded),
              config.smtp_access)


if __name__ == '__main__':
    main()
