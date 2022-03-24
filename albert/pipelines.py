# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

import scrapy.exporters
import scrapy.exceptions


class AlbertPipeline:
    def process_item(self, item, spider):
        return item


class CsvExport:
    def __init__(self):
        self.files = {}

    def open_spider(self, spider):
        write_to_file = open(spider.csv_export_file, 'wb')
        self.files[spider] = write_to_file
        self.exporter = scrapy.exporters.CsvItemExporter(file=write_to_file, fields_to_export=spider.fields)
        self.exporter.start_exporting()
