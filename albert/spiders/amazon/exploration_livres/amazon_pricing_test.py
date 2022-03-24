# #!/usr/bin/env python2.7
# # -*- coding: utf_8 -*-
#
# """Pricing PT: Scrapy spider for website Amazon.fr
#
# This script crawls the target website based on a list of EANs.
# Scraped data includes title, own offer price and own shipping price.
# The output is a CSV file where each line corresponds to an EAN.
# """
# from abc import ABC
# from builtins import str
# # from albator.spiders import core
# from albert import items
# import scrapy.http.request
# import pandas
# from scrapy import Spider
#
#
# class AmazonPricingSpider(Spider, ABC):
#     allowed_domains = ["amazon.fr"]
#     name = "amazon_pricing"
#     CONCURRENT_REQUESTS = 99
#     AUTOTHROTTLE_ENABLED = False
#     crawlera_enabled = True
#     handle_httpstatus_list = [403, 503, 429, 504, 502, 500, 408, 404, 503]
#     crawlera_apikey = '4caa50e38fc540848b4833fee1ed2d9d'  # de036e2828514430b9a331f4d7d84b53  dab0a655fa7845a6bc11639d5696fa86
#     RETRY_TIMES = 2
#     custom_settings = {
#         "RETRY_HTTP_CODES": [500, 502, 503, 504, 408, 404],
#         "CONCURRENT_REQUESTS": 99,
#         "DOWNLOADER_MIDDLEWARES": {
#             'scrapy_crawlera.CrawleraMiddleware': 610
#         },
#         "CLOSESPIDER_TIMEOUT": 64500
#     }
#
#     def __init__(self, project_name, crawl_name, input_filename, base_directory, today_ymd, exe_mode,
#                  prime=False, availability=False, sql_sku=True):
# TODO : virer le exe_mode
#
#         if sql_sku=False, then don't request filtered.{}.sku
#         if prime:
#             self.crawlera_apikey = 'de036e2828514430b9a331f4d7d84b53'
#         super(AmazonPricingSpider, self).__init__(project_name, crawl_name, input_filename, ["sku"], object,
#                                                   ["sku", "own_offer_price", "own_shipping_price",
#                                                    "marketplace_offer_price", "marketplace_shipping_price"],
#                                                   base_directory, today_ymd, "https://www.amazon.fr")
#         self.offer_listing_base_url = "/gp/offer-listing/"
#         self.prime = prime
#         self.sql_sku = sql_sku
#         self.availability = availability
#         self.not_found_skus_df = pandas.DataFrame(columns=["sku"])
#
#     def start_requests(self):
#         """
#         yields requests according to a list of amazon skus
#         :return:
#         """
#
#         sql = "select stored.amazon.sku from (SELECT filtered.{name}.ean, " \
#               "filtered.{name}.sku FROM filtered.{name}  LEFT OUTER JOIN stored.amazon_non_matched  " \
#               "ON ( filtered.{name}.ean = stored.amazon_non_matched.ean ) WHERE  " \
#               "stored.amazon_non_matched.ean IS NULL ) millieu LEFT JOIN stored.amazon " \
#               "on (millieu.ean = stored.amazon.ean)".format(name=self.project_name)
#         if self.sql_sku == "False":
#             sql = sql.replace(", filtered." + self.project_name + ".sku", "")
#         data = self.execute_query(sql, is_select=True)
#         skus_s = [row['sku'].strip() for row in data if row['sku']]
#
#         for sku in skus_s:
#             yield scrapy.http.request.Request(self.get_absolute_url(self.forge_url_from_sku(sku)),
#                                               callback=self.parse_offer_listing, meta={"sku": sku})
#
#     def forge_url_from_ean(self, ean):
#         """
#         :param ean:
#         :return:
#         """
#         pass
#
#     def forge_url_from_sku(self, sku):
#         """
#         :param sku:
#         :return:
#         """
#         return self.offer_listing_base_url + sku
#
#     def parse_ean_search(self, response):
#         """
#         :param response:
#         :return:
#         """
#         pass
#
#     def parse_offer_listing(self, response):
#         """
#         parse the web page that has the offer listing
#         :param response:
#         :return:
#         """
#
#         # This page is either not the first one or the first one
#         try:
#             item = response.meta["item"]
#         except KeyError:
#             item = items.PricingItem()
#             item["sku"] = str(response.meta["sku"])
#
#         if self.availability:
#             self.fields += ["availability"]
#             availability = response.xpath \
#                 ('normalize-space(//div[@id="availability"])').extract_first()
#             if availability.startswith(("Bientôt disponible", "En stock", "Habituellement expédié sous",
#                                         "Il ne reste plus que", "Temporairement en rupture de stock",
#                                         "Cet article paraîtra")):
#                 if availability.startswith("Cet article paraîtra"):
#                     if "." in availability:
#                         availability = availability.split('.')[0]
#                 item["availability"] = availability
#
#         offers_selector_list = response.xpath('//div[@class="a-row a-spacing-mini olpOffer"]')
#
#         # Dismiss if no offer was found
#         if not offers_selector_list:
#             self.logger.info("Failed to find any offer for sku: " + item["sku"])
#             try:
#                 self.insert_skus_non_matched(str(response.meta["sku"]))
#             except Exception as e:
#                 self.logger.error("Couldn't insert non matched: " + e.message)
#                 return
#
#         for offer_selector in offers_selector_list:
#             # Dismiss the offer if the product is not marked as new
#             if offer_selector.xpath(
#                     'normalize-space(div[@class="a-column a-span3 olpConditionColumn"]/div[@class="a-section a-spacing-small"]/span[@class="a-size-medium olpCondition a-text-bold"])').extract_first() != "Neuf":
#                 continue
#
#             # Dismiss the offer if it is exclusive for Amazon Prime
#             if offer_selector.xpath('.//span[@id="pe-olp-exclusive-for-prime-string"]'):
#                 self.logger.info("Offer is exclusive for Amazon Prime")
#                 if not self.prime:
#                     continue
#
#             # Determine whether the offer is Amazon or Marketplace
#             if offer_selector.xpath(
#                     'div[@class="a-column a-span2 olpSellerColumn"]/h3[@class="a-spacing-none olpSellerName"]/span[@class="a-size-medium a-text-bold"]/a'):
#                 is_marketplace = True
#             elif offer_selector.xpath(
#                     'div[@class="a-column a-span2 olpSellerColumn"]/h3[@class="a-spacing-none olpSellerName"]/img[@alt="Amazon.fr"]'):
#                 is_marketplace = False
#             else:
#                 self.logger.error("Technical: No seller name found")
#                 return
#
#             is_lowest_price_yet = False
#             is_lowest_price_too = False
#
#             # Get the offer price
#             offer_price = offer_selector.xpath(
#                 'normalize-space(div[@class="a-column a-span2 olpPriceColumn"]/span[@class="a-size-large a-color-price olpOfferPrice a-text-bold"])').extract_first()
#             if not offer_price:
#                 self.logger.error("Technical: No price found")
#                 continue
#
#             offer_price = "".join(offer_price.replace("EUR ", "").split("."))
#             if is_marketplace:
#                 try:
#                     item["marketplace_offer_price"]
#                 except KeyError:
#                     item["marketplace_offer_price"] = offer_price
#                     is_lowest_price_yet = True
#                 else:
#                     if float(offer_price.replace(",", ".")) < float(
#                             item["marketplace_offer_price"].replace(",", ".")):
#                         item["marketplace_offer_price"] = offer_price
#                         is_lowest_price_yet = True
#                     elif offer_price == item["marketplace_offer_price"]:
#                         is_lowest_price_too = True
#             else:
#                 try:
#                     item["own_offer_price"]
#                 except KeyError:
#                     item["own_offer_price"] = offer_price
#                     is_lowest_price_yet = True
#                 else:
#                     if float(offer_price.replace(",", ".")) < float(item["own_offer_price"].replace(",", ".")):
#                         item["own_offer_price"] = offer_price
#                         is_lowest_price_yet = True
#                     elif offer_price == item["own_offer_price"]:
#                         is_lowest_price_too = True
#
#             # Get the shipping price
#             if is_lowest_price_yet or is_lowest_price_too:
#                 shipping = offer_selector.xpath(
#                     'normalize-space(div[@class="a-column a-span2 olpPriceColumn"]//span[@class="a-color-secondary"])').extract_first()
#                 if not shipping:
#                     self.logger.error("Technical: No shipping found")
#                     if is_lowest_price_yet:
#                         if is_marketplace:
#                             item["marketplace_shipping_price"] = None
#                         else:
#                             item["own_shipping_price"] = None
#                 else:
#                     if "Livraison gratuite dès EUR 25" in shipping or "Livraison à EUR 0,01 sur les livres et gratuite dès EUR 25 d'achats sur tout autre article" in shipping:
#                         if is_marketplace:
#                             if float(item["marketplace_offer_price"].replace(",", ".")) < 25.0:
#                                 if is_lowest_price_yet or (
#                                         is_lowest_price_too and item["marketplace_shipping_price"] and float(
#                                     item["marketplace_shipping_price"].replace(",", ".")) > 2.79):
#                                     item["marketplace_shipping_price"] = "2,79"
#                             else:
#                                 item["marketplace_shipping_price"] = "0,00"
#                         else:
#                             if float(item["own_offer_price"].replace(",", ".")) < 25.0:
#                                 if is_lowest_price_yet or (
#                                         is_lowest_price_too and item["own_shipping_price"] and float(
#                                     item["own_shipping_price"].replace(",", ".")) > 2.79):
#                                     item["own_shipping_price"] = "2,79"
#                             else:
#                                 item["own_shipping_price"] = "0,00"
#                     elif "Livraison GRATUITE" in shipping or "Livraison gratuite en France métropolitaine" in shipping:
#                         if is_marketplace:
#                             item["marketplace_shipping_price"] = "0,00"
#                         else:
#                             item["own_shipping_price"] = "0,00"
#                     elif "de frais de livraison" in shipping or "+" in shipping:
#                         shipping_price = offer_selector.xpath(
#                             'normalize-space(div[@class="a-column a-span2 olpPriceColumn"]//span[@class="a-color-secondary"]/span[@class="olpShippingPrice"])').extract_first()
#                         if not shipping_price:
#                             self.logger.error("Technical: Other case for shipping #1")
#                             if is_marketplace:
#                                 item["marketplace_shipping_price"] = None
#                             else:
#                                 item["own_shipping_price"] = None
#                         else:
#                             shipping_price = shipping_price.replace("EUR ", "")
#                             if is_marketplace:
#                                 if is_lowest_price_yet or (
#                                         is_lowest_price_too and item["marketplace_shipping_price"] and float(
#                                     item["marketplace_shipping_price"].replace(",", ".")) > float(
#                                     shipping_price.replace(",", "."))):
#                                     item["marketplace_shipping_price"] = shipping_price
#                             else:
#                                 if is_lowest_price_yet or (
#                                         is_lowest_price_too and item["own_shipping_price"] and float(
#                                     item["own_shipping_price"].replace(",", ".")) > float(
#                                     shipping_price.replace(",", "."))):
#                                     item["own_shipping_price"] = shipping_price
#                     else:
#                         if "+ expédition (actuellement indisponible)" not in shipping:
#                             self.logger.error("Technical: Other case for shipping #2")
#                         if is_marketplace:
#                             item["marketplace_shipping_price"] = None
#                         else:
#                             item["own_shipping_price"] = None
#
#         # Go to the next page if there is one, otherwise yield the item if actual data was found
#         next_page_url_path = response.xpath(
#             '//div[@class="a-text-center a-spacing-large"]/ul[@class="a-pagination"]/li[@class="a-last"]/a[text() = "Suivant"]/@href').extract_first()
#         if next_page_url_path is None:
#             # Dismiss the item if no offer price was found
#             try:
#                 item["own_offer_price"]
#             except KeyError:
#                 try:
#                     item["marketplace_offer_price"]
#                 except KeyError:
#                     self.logger.error("Failed to find any offer price")
#                     return
#
#             yield item
#         else:
#             yield scrapy.http.request.Request(self.get_absolute_url(next_page_url_path),
#                                               callback=self.parse_offer_listing, meta={"item": item})
#
#
# if __name__ == "__main__":
#     pass
