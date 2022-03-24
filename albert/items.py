# -*- coding: utf-8 -*-

# Define here the models for your scraped item
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class AlbertItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class PricingItem(scrapy.item.Item):
    sku = scrapy.item.Field()
    ean = scrapy.item.Field()
    own_offer_price = scrapy.item.Field()
    own_shipping_price = scrapy.item.Field()
    marketplace_offer_price = scrapy.item.Field()
    marketplace_shipping_price = scrapy.item.Field()
    stock_mp = scrapy.item.Field()
    ranking = scrapy.item.Field()
    rating = scrapy.item.Field()
    availability = scrapy.item.Field()
