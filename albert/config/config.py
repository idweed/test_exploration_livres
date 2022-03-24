#!/usr/bin/env python3.7

"""
Basic Configuration File for crawl project
"""

from pathlib import Path

env = 'prod'
# env = 'DEV'

class Access(object):
    def __init__(self, host, username, password, directory=None):
        self.host = host
        self.username = username
        self.password = password
        self.directory = directory


smtp_access = Access('smtp.office365.com:587', 'crawl-diffusion@fnacdarty.com', 'Amazon!Robot007!')
fnac_deposit_ftp_access = Access('ftpfnac01.fnac.com', 'part_crawl', 'agA6eE71', 'CRAWL')
ecomstat_ftp_access = Access('ecomstatprd1.intranet.darty.fr','statapp', 'D@rty2o17', 'home/statapp/boulanger')


SHUB_APIKEY = 'ccb81b6cecba4481baa8ff6dbe8c03e0'
KEEPA_APIKEY = '46bt1moa68mkpnn1s2844i7239dm8d28ua8mv2q40o20da062qgmulnsreim5eev'
PROJECT_ID = '436828'

BASE_DIR = Path(__file__).resolve().parent.parent
# print(root_project_dir)
