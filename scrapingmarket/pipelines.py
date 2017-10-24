# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

#from pymongo import MongoClient
from scrapy.exceptions import DropItem
import re
import pydocumentdb
import pydocumentdb.document_client as document_client


config = { 
    'ENDPOINT': '<<end point url>>',
    'MASTERKEY': '<<master key>>',
    'DOCUMENTDB_DATABASE': 'scraping-book',
    'DOCUMENTDB_COLLECTION': 'market-operations'
};

class ValidationPipline(object):
    """
    itemを検証するPipline
    """
    def process_item(self, item, spider):
        if not item['title']:
            raise DropItem('Missing title')
        return item


class MongoPipeline(object):

    def open_spider(self, spider):
        # Initialize the Python DocumentDB client
        self.client = document_client.DocumentClient(config['ENDPOINT'], {'masterKey': config['MASTERKEY']})
        # create a database if not yet created
        self.database_definition = {'id': config['DOCUMENTDB_DATABASE'] }
        self.databases = list(self.client.QueryDatabases({
                'query': 'SELECT * FROM root r WHERE r.id=@id',
                'parameters': [
                    { 'name':'@id', 'value': self.database_definition['id'] }
                ]
            }))
        if ( len(self.databases) > 0 ):
            self.db = self.databases[0]
        else:
            print ("database is created:%s" % config['DOCUMENTDB_DATABASE'])
            self.db = self.client.CreateDatabase(self.database_definition)

        # Create collection options
        self.options = {
            'offerEnableRUPerMinuteThroughput': True,
            'offerVersion': "V2",
            'offerThroughput': 400
        }
        # create a collection if not yet created
        self.collection_definition = { 'id': config['DOCUMENTDB_COLLECTION'] }
        self.collections = list(self.client.QueryCollections(
            self.db['_self'],
            {
                'query': 'SELECT * FROM root r WHERE r.id=@id',
                'parameters': [
                    { 'name':'@id', 'value': self.collection_definition['id'] }
                ]
            }))
        if ( len(self.collections) > 0 ):
            self.collection = self.collections[0]
        else:
            print ("collection is created:%s" % config['DOCUMENTDB_COLLECTION'])
            self.collection = self.client.CreateCollection(self.db['_self'], self.collection_definition, self.options)

        ### Mongo
        #uri = "mongodb://scraping-pool-mongoXXX"
        #self.client = MongoClient(uri)
        #self.db = self.client['scraping-book']
        #self.collection = self.db['items']

    def process_item(self, item, spider):
        # Create some documents
        self.data = {
            'date':item['date'],
            'title':item['title'],
            'header':item['header'],
            'offer':item['offer'],
            'url':item['url']
        }
        # check if duplicated
        self.documents = list(self.client.QueryDocuments(
            self.collection['_self'],
            {
                'query': 'SELECT * FROM root r WHERE r.title=@title',
                'parameters': [
                    { 'name':'@title', 'value':self.data['title'] }
                ]
            }))
        if (len(self.documents) < 1):
            # only create if it's fully new document
            print ("document is added:title: %s" % self.data['title'])
            self.created_document = self.client.CreateDocument(
                    self.collection['_self'], self.data)
        
        ### Mongo
        #self.collection.insert_one(dict(item))
            #return item