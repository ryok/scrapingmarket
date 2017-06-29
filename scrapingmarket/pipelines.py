# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
from pymongo import MongoClient
from scrapy.exceptions import DropItem

config = { 
    'ENDPOINT': 'https://scraping-pool-documentdb.documents.azure.com:443/',
    'MASTERKEY': 'XSgJ6wV0b6a6vIpOf4aAHKvvgWRVRP78FgtKGRS83GIHokyKCRvaadkOARGVHUFXlsJfuZDWplpOGdpSzOqUzg==',
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
        #uri = "mongodb://scraping-pool-mongo:5rq1tRCSzkr6algHaLa9FZ4aFKGNcZ6FJgq9Z182fpiSxvKspf42wOGUoHPvHaq3NUMRunAcuHO1rTxUVeYjkg==@scraping-pool-mongo.documents.azure.com:10255/?ssl=true&replicaSet=globaldb"
        #self.client = MongoClient(uri)
        #self.db = self.client['scraping-book']
        #self.collection = self.db['items']
        # Initialize the Python DocumentDB client
        client = document_client.DocumentClient(config['ENDPOINT'], {'masterKey': config['MASTERKEY']})
        # create a database if not yet created
        database_definition = {'id': config['DOCUMENTDB_DATABASE'] }
        databases = list(client.QueryDatabases({
                'query': 'SELECT * FROM root r WHERE r.id=@id',
                'parameters': [
                    { 'name':'@id', 'value': database_definition['id'] }
                ]
            }))
        if ( len(databases) > 0 ):
            db = databases[0]
        else:
            print ("database is created:%s" % config['DOCUMENTDB_DATABASE'])
            db = client.CreateDatabase(database_definition)

        # Create collection options
        options = {
            'offerEnableRUPerMinuteThroughput': True,
            'offerVersion': "V2",
            'offerThroughput': 400
        }
        # create a collection if not yet created
        collection_definition = { 'id': config['DOCUMENTDB_COLLECTION'] }
        collections = list(client.QueryCollections(
            db['_self'],
            {
                'query': 'SELECT * FROM root r WHERE r.id=@id',
                'parameters': [
                    { 'name':'@id', 'value': collection_definition['id'] }
                ]
            }))
        if ( len(collections) > 0 ):
            collection = collections[0]
        else:
            print ("collection is created:%s" % config['DOCUMENTDB_COLLECTION'])
            collection = client.CreateCollection(db['_self'], collection_definition, options)

    def close_spider(self, spider):
        #self.client.close()

    def process_item(self, item, spider):
        #self.collection.insert_one(dict(item))
        #return item
        # Create some documents
        data = {
            'date':item['title'],
            'title':item['title'],
            'offer':item['offer']
        }
        # check if duplicated
        documents = list(client.QueryDocuments(
            collection['_self'],
            {
                'query': 'SELECT * FROM root r WHERE r.title=@title',
                'parameters': [
                    { 'name':'@title', 'value':data['title'] }
                ]
            }))
        if (len(documents) < 1):
            # only create if it's fully new document
            print ("document is added:title: %s" % data['title'])
            created_document = client.CreateDocument(
                    collection['_self'], data)