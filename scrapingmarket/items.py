# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# http://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class OpeOffer(scrapy.Item):
    date = scrapy.Field()
    url = scrapy.Field()
    title = scrapy.Field()
    offer = scrapy.Field()
    