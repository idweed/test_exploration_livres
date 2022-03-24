#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-

"""
Content of test_core.py
This test suite is made for core.py file, which contains spiders core functions
"""

import unittest
import requests
import pytest
from albert.spiders.core import CoreSpider
from albert.config.config import SHUB_APIKEY, PROJECT_ID


@pytest.mark.usefixtures()
class CoreTest(unittest.TestCase):

    @staticmethod
    def test_CoreSpider_success():
        data = '{"fp":"/some/path1.html"}\n{"fp":"/some/path2.html"}\n {"fp":"/some/path3.html"}'
        try:
            requests.post(f'https://storage.scrapinghub.com/hcf/{PROJECT_ID}/test/s/example.com',
                          data=data, auth=(SHUB_APIKEY, ''))
        except Exception as err:
            raise ConnectionError('in test_CoreSpider : requests not working - test failed')
        storage_endpoint_url = f'https://storage.scrapinghub.com/hcf/{PROJECT_ID}/test/s/example.com/q'
        class_spider = CoreSpider('project_name', 'fields', storage_endpoint_url, 'base_directory', 'root_url',
                                  'today_ymd', 'dataname')
        try:
            requests.delete(f'https://storage.scrapinghub.com/hcf/{PROJECT_ID}/test/s/example.com/',
                            auth=(SHUB_APIKEY, ''))
        except Exception as err:
            raise ConnectionError('in test_CoreSpider : delete request not working - test failed')
        assert isinstance(class_spider, CoreSpider)

    @staticmethod
    def test_CoreSpider_isEAN_fail():
        data = '{"fp":"/some/path1.html"}\n{"fp":"/some/path2.html"}\n {"fp":"/some/path3.html"}'
        try:
            requests.post(f'https://storage.scrapinghub.com/hcf/{PROJECT_ID}/test/s/example.com',
                          data=data, auth=(SHUB_APIKEY, ''))
        except Exception as err:
            raise ConnectionError('in test_CoreSpider : requests not working - test failed')
        storage_endpoint_url = f'https://storage.scrapinghub.com/hcf/{PROJECT_ID}/test/s/example.com/q'
        class_spider = CoreSpider('project_name', 'fields', storage_endpoint_url, 'base_directory', 'root_url',
                                  'today_ymd', 'dataname', crawl_type=False)
        try:
            requests.delete(f'https://storage.scrapinghub.com/hcf/{PROJECT_ID}/test/s/example.com/',
                            auth=(SHUB_APIKEY, ''))
        except Exception as err:
            raise ConnectionError('in test_CoreSpider : delete request not working - test failed')
        assert isinstance(class_spider, CoreSpider)

    @staticmethod
    def test_CoreSpider_fail_argument_missing():
        with pytest.raises(TypeError):
            CoreSpider('project_name', 'fields', 'toto', 'titi', 'root')

    @staticmethod
    def test_CoreSpider_fail_connection_error():
        with pytest.raises(ValueError):
            garbage_id = 1234
            storage_endpoint_url = f'https://storage.scrapinghub.com/hcf/{garbage_id}/test/s/example.com/q'
            CoreSpider('project_name', 'fields', storage_endpoint_url, 'base_directory', 'root_url',
                       'today_ymd')


if __name__ == '__main__':
    unittest.main()
