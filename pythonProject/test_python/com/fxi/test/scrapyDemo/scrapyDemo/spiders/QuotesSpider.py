import scrapy
import logging

class QuotesSpider(scrapy.Spider):
    name = "quotes"

    allowed_domains = ["taobao.com"]

    start_urls = ['https://shopsearch.taobao.com/search?app=shopsearch&q=%E9%9F%A9%E5%9B%BD%E4%BB%A3%E8%B4%AD&imgfile=&js=1&stats_click=search_radio_all%3A1&initiative_id=staobaoz_20170921&ie=utf8&sort=credit-desc']

    def parse(self, response):
        body=response.body.decode("utf-8","ignore")
        logging.debug(body)
        item = {}
        item["title"] = 'aa'
        yield item