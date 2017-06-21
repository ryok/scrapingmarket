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
        for trtext in response.css('tr').extract():
            for tdtext in response.css('td::text').extract():
                item['offer'] = tdtext
        yield item
