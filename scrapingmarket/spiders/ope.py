# -*- coding: utf-8 -*-
import scrapy
from scrapingmarket.items import OpeOffer


class OpeSpider(scrapy.Spider):
    name = 'ope'
    allowed_domains = ['www3.boj.or.jp']
    # start_urls = ['http://www3.boj.or.jp/market/jp/menu_o.htm']
    start_urls = ['http://www3.boj.or.jp/market/jp/menuold_o_2017.htm']

    def parse(self, response):
        for url in response.css('td a::attr("href")').re(r'stat/ba\d+.htm$'):
            yield scrapy.Request(response.urljoin(url), self.parse_opes)

    def parse_opes(self, response):
        """
        ページからオペの内容を抜き出す
        """
        item = OpeOffer()
        item['url'] = response.url
        item['title'] = response.css('title::text').extract()
        item['offer'] = response.css('td::text').extract()
        #num = 1
        #for tr in response.css('tr::text').extract():
        #    itemKey = 'offer'
        #    item[itemKey] = tr
        #    num+=1
        yield item
