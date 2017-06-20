# -*- coding: utf-8 -*-
import scrapy


class OpeSpider(scrapy.Spider):
    name = 'ope'
    allowed_domains = ['www3.boj.or.jp/market/jp/menu_o.htm']
    start_urls = ['http://www3.boj.or.jp/market/jp/menu_o.htm']

    def parse(self, response):
        # print(response.css('ul a::attr("href")').extract())
        for url in response.css('ul a::attr("href")').re(r'stat/of\d+.htm$'):
            yield scrapy.Request(response.urljoin(url), self.parse_opes)

    def parse_opes(self, response):
        pass