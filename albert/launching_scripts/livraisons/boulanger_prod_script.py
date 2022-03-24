#!/usr/bin/env python3
from albert.config.config import ecomstat_ftp_access
import albert.config.config as config
import albert.config.ftp_wrapper as ftp
import logging
import datetime
import os


def transfer_file(hour='08'):
    if hour in ['08', '14']:
        date = datetime.datetime.now().strftime('%Y-%m-%d')
        filename = 'boulanger_{}-{}.csv'.format(date, hour)
        tmppath = '/tmp/{}'.format(filename)
        logging.basicConfig(filename='/tmp/boulanger_prod_{}.log'.format(date), level='DEBUG')
        try:
            os.remove(tmppath)
        except Exception as err:
            logging.info('cannot remove tmppath as it does not exist - {}'.format(err))
        # telechargement du fichier present sur fnacdeposit vers le repertoire tmp
        if ftp.download_file(config.fnac_deposit_ftp_access, tmppath, filename) is True:
            # upload vers le serveur ecomstat
            ftp.upload(ecomstat_ftp_access, tmppath)
        else:
            raise ValueError('File not uploaded, please try again')
