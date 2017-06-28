# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from pymongo import MongoClient
from scrapy.exceptions import DropItem

class ValidationPipline(object):
    ""
    itemを検証するPipline
    ""
    def process_item(self, item, spider):
        if not item['title']:
            raise DropItem('Missing title')
        return item

class ScrapingmarketPipeline(object):
    def process_item(self, item, spider):
        return item

class MongoPipeline(object):

    def open_spider(self, spider):
        uri = "mongodb://scraping-pool-mongo:5rq1tRCSzkr6algHaLa9FZ4aFKGNcZ6FJgq9Z182fpiSxvKspf42wOGUoHPvHaq3NUMRunAcuHO1rTxUVeYjkg==@scraping-pool-mongo.documents.azure.com:10255/?ssl=true&replicaSet=globaldb"
        self.client = MongoClient(uri)
        self.db = self.client['scraping-book']
        self.collection = self.db['items']

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        self.collection.insert_one(dict(item))
        return item