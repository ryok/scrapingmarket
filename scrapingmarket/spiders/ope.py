# -*- coding: utf-8 -*-
import re
import scrapy
from scrapingmarket.items import OpeOffer


class OpeSpider(scrapy.Spider):
    name = 'ope'
    allowed_domains = ['www3.boj.or.jp']
    start_urls = ['http://www3.boj.or.jp/market/jp/menuold_o_2017.htm']

    def parse(self, response):
        for url in response.css('td a::attr("href")').re(r'stat/ba\d+.htm$'):
            yield scrapy.Request(response.urljoin(url), self.parse_opes)

    def parse_opes(self, response):
        """
        ページからオペの内容を抜き出す
        """
        item = OpeOffer()
        item['date'] = re.sub('^ba','20',re.sub('.htm$','',response.url.split('/')[-1]) )
        item['title'] = response.css('title::text').extract()
        item['header'] = response.css('th::text').extract()
        item['offer'] = response.css('td::text').extract()
        item['url'] = response.url
        yield item
