# -*- coding: utf-8 -*-
import scrapy
from scrapingmarket.items import OpeOffer


class OpeSpider(scrapy.Spider):
    name = 'ope'
    allowed_domains = ['www3.boj.or.jp']
    start_urls = ['http://www3.boj.or.jp/market/jp/menu_o.htm']

    def parse(self, response):
        for url in response.css('ul a::attr("href")').re(r'stat/of\d+.htm$'):
            yield scrapy.Request(response.urljoin(url), self.parse_opes)

    def parse_opes(self, response):
        """
        ページからオペの内容を抜き出す
        """
        item = OpeOffer()
        item['title'] = response.css('title::text').extract()
        item['bid'] = response.css('td::text').extract()
        #num = 1
        #for tr in response.css('tr::text').extract():
        #    itemKey = 'offer'
        #    item[itemKey] = tr
        #    num+=1
        yield item
