#!/usr/bin/env python3.7
# -*- coding: utf-8 -*-

"""Scrapy base spider
The purpose of this script is to provide a generic spider in the form of an abstract class to derive from.
"""
from abc import ABC
from builtins import str
import abc

import pandas as pd
import numpy as np
import scrapy.crawler
import scrapy.http.request
import scrapy.spiders.crawl
import scrapy.utils.project
import requests
from albert.config.config import SHUB_APIKEY
from json import loads


class CoreSpider(scrapy.Spider, ABC, metaclass=abc.ABCMeta):

    def __init__(self, project_name, fields, storage_endpoint_url, base_directory, root_url,
                 today_ymd, dataname, crawl_type='EAN', **kwargs):
        super(CoreSpider, self).__init__(name='Cora', **kwargs)
        self.project_name = project_name
        self.fields = fields
        self.base_directory = base_directory
        self.today_ymd = today_ymd
        self.root_url = root_url
        try:
            csv_data = requests.get(storage_endpoint_url, auth=(SHUB_APIKEY, ''))
            csv_data.raise_for_status()
            if crawl_type == 'EAN':
                try:
                    arr = [x for x, y in loads(csv_data.text)['requests']]
                    self.df = pd.DataFrame(data=arr, columns=[dataname])
                except Exception as err:
                    raise ValueError(f"Couldn't create dataframe : {err}")
        except requests.exceptions.HTTPError as err:
            raise ValueError(f"Scrapinghub frontier storage HTTP error : {err}")
        except Exception as err:
            raise ValueError(f"CoreSpider error encountered : {err}")


# class EanBaseSpider(BaseSpider):
#     def __init__(self, project_name, crawl_name, input_filename, input_columns, column_types, fields, base_directory,
#                  today_ymd, root_url, ignore_non_matched=False):
#         super(EanBaseSpider, self).__init__(project_name, crawl_name, fields, base_directory, today_ymd, root_url)
#         self.input_filename = input_filename
#         self.input_columns = input_columns
#         self.column_types = column_types
#         self.crawl_name = crawl_name
#         self.table_name_non_matched = "stored." + self.crawl_name + "_non_matched"
#         self.ignore_non_matched = ignore_non_matched
#
#     def insert_skus_non_matched(self, sku):
#         self.not_found_skus_df = self.not_found_skus_df.append(
#             {"sku": sku}, ignore_index=True)
#         self.not_found_skus_df = self.not_found_skus_df.drop_duplicates(subset="sku")
#         if self.not_found_skus_df['sku'].size >= 1:
#             try:
#                 cur = self.conn.cursor(dictionary=True)
#                 sql = "INSERT into {}(sku) Values(%s)".format(
#                     self.table_name_non_matched.replace("non_matched", "sku_non_matched"))
#                 cur.executemany(sql, [tuple(x) for x in self.not_found_skus_df.values])
#                 self.conn.commit()
#                 self.not_found_skus_df = pandas.DataFrame(columns=["sku"])
#             except Exception as e:
#                 self.logger.error("INSERT NON_MATCHED FAILED, error: {}".format(e))
#                 pass
#
#     def insert_non_matched(self, ean):
#         self.not_found_eans_df = self.not_found_eans_df.append(
#             {"ean": ean}, ignore_index=True)
#         self.not_found_eans_df = self.not_found_eans_df.drop_duplicates(subset="ean")
#         if self.not_found_eans_df['ean'].size >= 1:
#             try:
#                 cur = self.conn.cursor(dictionary=True)
#                 sql = "INSERT into {}(ean) Values(%s)".format(self.table_name_non_matched)
#                 cur.executemany(sql, [tuple(x) for x in self.not_found_eans_df.values])
#                 self.conn.commit()
#                 self.not_found_eans_df = pandas.DataFrame(columns=["ean"])
#             except Exception as e:
#                 self.logger.error("INSERT NON_MATCHED FAILED, error: {}".format(e))
#                 pass
#
#     @abc.abstractmethod
#     def forge_url_from_ean(self, ean):
#         pass
#
#     def start_requests(self):
#         # Load the EANs to crawl
#         if self.project_name in ["precommandes_audio", "precommandes_video"]:
#             db_name = "scraped"
#         else:
#             db_name = "filtered"
#         # if self.project_name in [u"camera"]:
#         #
#         db_table = db_name + "." + self.project_name
#
#         self.logger.info("crawl_name: " + self.crawl_name)
#         self.logger.info("project_name: " + self.project_name)
#         self.logger.info("input_filename: " + self.input_filename)
#         self.logger.info("db_table: " + db_table)
#
#
#         try:
#             ean_s = self.select_table(db_table, self.input_columns, as_df=True)  # TO-DO # pandas.read_sql
#         except Exception as e:
#             self.logger.error("Could not get eans from core spider: {}".format(e))
#
#         # get brands for cdiscount spider
#         if self.crawl_name == "cdiscount":
#             if self.brands:
#                 try:
#                     file_brand_name = "MARQUE" if "pricing_pt_darty_filtered" in self.input_filename else "Editeur"
#                     brands = ean_s[["ean", file_brand_name]].copy()
#                 except:
#                     pass
#         ean_s = ean_s["ean"]
#         print(("EANS Size", ean_s.size))
#         # Drop eans that couldn't be found on previous runs
#         if not self.ignore_non_matched:
#             try:
#                 db_table = "stored." + self.crawl_name + "_non_matched"
#                 self.logger.info("working")
#                 stored_non_matched = self.select_table(db_table, ["ean"], as_df=True)
#                 self.logger.info("working")
#                 ean_s = ean_s.loc[~ean_s.isin(stored_non_matched['ean'])]
#                 self.logger.info("working")
#             except Exception as e:
#                 self.logger.error("Couldn't read {} non_matched stored file".format(self.crawl_name))
#                 self.logger.error("non matched eans should go in the following table:\n{}".format(db_table))
#                 self.logger.error("here is your error :\n{}".format(e))
#
#         # Load the stored EANs and SKUs if applicable
#         try:
#             sku_df = pandas.read_sql(
#                 "SELECT ean, {} from {} where ean in {}".format(self.sku, "stored." + self.crawl_name, tuple(ean_s)),
#                 self.conn)
#         # Case: spider with no stored SKUs
#         except Exception as e:
#             self.logger.debug("Couldn't find skus in core spider: {}".format(e))
#         else:
#             self.logger.debug("Read data from table: ")
#         try:
#             callback_function = self.parse_offer_listing
#         except AttributeError:
#             try:
#                 callback_function = self.parse_detailed_offer
#             except AttributeError:
#                 pass
#         for ean in ean_s.drop_duplicates().tolist():
#             print(ean)
#             values = {"ean": ean}
#             if self.crawl_name == 'studiosport':
#                 self.logger.info("core : forge studiosport url")
#                 try:
#                     hashid = "e3f246b7c64f264f39d86726ab2d7a7a"
#                     product_url = "https://eu1-search.doofinder.com/5/search?hashid=" + str(hashid) + \
#                                   "&query_counter=15&page=1&rpp=5&transformer=&type=df_suggestions&query_name=starts_with_prefix&nostats=1&query=" \
#                                   + str(ean)
#                     yield scrapy.http.request.Request(
#                         product_url,
#                         callback=self.parse_ean_search,
#                         headers={"Sec-Fetch-Mode": "cors", "Origin": "https://www.studiosport.fr"},
#                         meta={"ean": ean}, dont_filter=True)
#                     continue
#                     # scrapy.http.request.Request(self.get_absolute_url(self.forge_url_from_ean(ean)),
#                     #                             callback=self.parse_ean_search, meta=values,
#                     #                             dont_filter=True)
#                 except:
#                     continue
#             if self.crawl_name == "cdiscount":
#                 if self.brands:
#                     try:
#                         brand = brands.loc[brands['ean'] == ean, file_brand_name].tolist()[0]
#                         values["brand"] = brand
#                     except:
#                         pass
#             try:
#                 specific_sku_s = sku_df.loc[sku_df["ean"] == ean, self.sku]
#             except UnboundLocalError:
#                 self.logger.info("core : forge url from ean - parse_ean_search")
#                 yield scrapy.http.request.Request(self.get_absolute_url(self.forge_url_from_ean(ean)),
#                                                   callback=self.parse_ean_search, meta=values)
#                 # headers={'Referer': 'https://www.google.fr/search?q=' + str(ean)})
#                 continue
#
#             try:
#                 sku = specific_sku_s.item()
#             except ValueError:
#                 self.logger.info("core : forge url from ean - parse_ean_search")
#                 yield scrapy.http.request.Request(self.get_absolute_url(self.forge_url_from_ean(ean)),
#                                                   callback=self.parse_ean_search, meta=values)
#                 # headers={'Referer': 'https://www.google.fr/search?q=' + str(ean)})
#                 continue
#
#             if specific_sku_s.isnull().item():
#                 self.logger.error("Found EAN with no SKU in local database: " + ean)
#                 continue
#             yield scrapy.http.request.Request(self.get_absolute_url(self.forge_url_from_sku(sku)),
#                                               callback=callback_function, meta={"ean": ean})
#             # headers={'Referer': 'https://www.google.fr/search?q=' + str(ean)})
#
#     @abc.abstractmethod
#     def parse_ean_search(self, response):
#         pass
#

if __name__ == "__main__":
    pass
