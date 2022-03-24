# -*- coding: utf-8 -*-

from builtins import object
import logging
import random
from w3lib.http import basic_auth_header


# import config

class Proxymesh(object):

    def __init__(self):
        logging.debug('Initialized Proxymesh middleware')
        # self.ip_locations = ['fr', 'au', 'de', 'nl', 'sg', 'uk', 'open', 'world']
        self.ip_locations = ['fr', 'de', 'nl', 'uk']

    def process_request(self, request, spider):
        proxy_ip_location = random.choice(self.ip_locations)
        # edge case for erwin mayer
        if "erwinmayer" in request.url:
            location = "fr"
        elif "boulanger" in request.url:
            location = "uk"
        else:
            location = proxy_ip_location
        # request.meta['proxy'] = u'http://' + config.proxymesh_auth + u'@' + location + u'.proxymesh.com:31280'
        request.meta['proxy'] = 'http://' + location + '.proxymesh.com:31280'
        request.headers['Proxy-Authorization'] = basic_auth_header('OPENVALUE', 'robot!iloveamazon12')
        logging.debug('Processing request through proxy IP: ' + request.meta['proxy'] + ' to URL: ' + request.url)
