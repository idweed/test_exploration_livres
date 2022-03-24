#!/usr/bin/env python3
import logging
import datetime
import albert.config.config as config
import sys
from time import sleep
from scrapinghub import ScrapinghubClient


class BaseScrapinghubScript:
    def __init__(self, **kwargs):
        self.project_name = kwargs['project_name']
        self.today_ymd = datetime.datetime.now().strftime('%Y%m%dT%H')
        self.client = None
        self.job_key = '436828/3/95'
        self.job_keys = []

    def logger(self, logging_level='DEBUG'):
        logger_filename = config.BASE_DIR / 'logs' / f'{self.project_name}_{self.today_ymd}.log'
        try:
            logging.basicConfig(filename=logger_filename,
                                format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
                                level=logging_level)
            logging.info('script initiated')
        except Exception as err:
            print(f'{self.project_name} : ERROR CREATING LOGGER at location : {logger_filename}')
            print(err)
            print(f'quitting project')
            sys.exit(1)

    def connect_to_client(self, client, apikey, name, number_of_tries=5, timeout=10):
        if number_of_tries < 1:
            logging.error('Number of tries : value is too low - program quitting')
            raise ValueError(f'ERROR : number of tries is incorrect. Cannot connect to : {name}')
        for i in range(0, number_of_tries):
            try:
                if apikey:
                    logging.info('Connecting to {} api using key {}***'.format(name, apikey[:4]))
                    return client(apikey)
                else:
                    logging.info('Connecting to client {}, no key required'.format(name))
                    return client()
            except Exception as e:
                logging.info("TRY [{}]: Couldn't get {}, trying again.\n{}".format(i, name, e))
                sleep(timeout)
        raise ValueError("ERROR: Couldn't get {} after {} tries".format(name, number_of_tries))

    def connect_to_scrapinghub(self):
        client_name = 'scrapinghub'
        self.client = self.connect_to_client(ScrapinghubClient, config.SHUB_APIKEY, client_name)
        if self.client:
            logging.info(f'Login successful to {client_name}')
        else:
            logging.error('Login failure, quitting')
            raise 'Connection error to {} -- {}'.format(client_name, ConnectionError)

    def wait_for_spiders(self, job_keys, project, minutes):
        i = 0
        for job_key in job_keys:
            try:
                while project.jobs.get(job_key).metadata.get('state') != 'finished':
                    logging.info(f'Job is not finished, sleeping for {minutes} min')
                    sleep(minutes * 60)
            except Exception:
                i += 1
                logging.error(f'Sleeping for 60 seconds, server error')
                sleep(60)
                if i == 5:
                    logging.error(f'quitting, server error not resolved')
                    break
            logging.info(f'Crawler for {job_key} ended')

    def run_scrapinghub_spider(self, project_id, spider_name, minutes=15):
        try:
            project = self.client.get_project(project_id)
        except Exception as err:
            logging.error(f'Connection failure to project id : {project_id}')
            raise err
        try:
            spider = project.spiders.get(spider_name)
            job_key = spider.jobs.run(units=1, meta={"spider_name": spider_name}).key
            if job_key:
                self.job_key = job_key
                while project.jobs.get(job_key).metadata.get('state') != 'finished':
                    logging.info(f'Job is not finished, sleeping for {minutes} min')
                    sleep(minutes * 60)
                logging.info(f'Crawler for {spider_name} ended')
        except Exception as err:
            logging.error('spider connection failure')
            raise err

    def run_scrapinghub_spider_list(self, project_id, spider_list, minutes=5):
        try:
            project = self.client.get_project(project_id)
        except Exception as err:
            logging.error(f'Connection failure to project id: {project_id}')
            raise err
        try:
            spiders = [(project.spiders.get(spider_name), spider_name) for spider_name in spider_list]
            job_keys = [spider.jobs.run(units=1, meta={'spider_name': name}).key for spider, name in spiders]
            if job_keys:
                self.job_keys = job_keys
                self.wait_for_spiders(job_keys, project, minutes)
        except Exception as err:
            logging.error('spider connection failure')
            raise err

    def get_scrapinghub_job_data(self, job_key):
        try:
            job = self.client.get_job(job_key)
            return job.items.list()
        except Exception as err:
            logging.error(f'Job data unavailable for {job_key}')
            raise err

    def get_scrapinghub_jobs_data(self, job_keys, spider_names):
        d = {spider_name: [] for spider_name in spider_names}
        try:
            for job_key in job_keys:
                job = self.client.get_job(job_key)
                d[job.metadata.get('spider')] = job.items.list()
            return d
        except Exception as err:
            logging.error(f'Job data unavailable for {spider_names}')
            raise err
