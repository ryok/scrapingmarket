# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from pymongo import MongoClient


class ScrapingmarketPipeline(object):
    def process_item(self, item, spider):
        return item

class MongoPipeline(object):

    def open_spider(self, spider):
        self.client = MongoClient('https://scraping-pool-mongo.documents.azure.com', 443)
        self.db = self.client['scraping-book']
        self.collection = self.db['items']

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        self.collection.insert_one(dict(item))
        return item