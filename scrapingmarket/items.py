# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ScrapingmarketItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass

class Headline(scrapy.Item):
    title = scrapy.Field()
    body = scrapy.Field()

class OpeOffer(scrapy.Item):
    offer = scrapy.Field()
    