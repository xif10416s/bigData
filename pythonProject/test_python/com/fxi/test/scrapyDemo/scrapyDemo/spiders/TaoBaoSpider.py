# -*- coding: utf-8 -*-
import scrapy
import logging
import time
from scrapy.http import Request
import re
import codecs
import os
import requests

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class QuotesSpider(scrapy.Spider):
    name = "taobao"

    allowed_domains = ["taobao.com"]

    start_urls = ['https://taobao.com/']

    day = str(time.strftime("%Y%m%d", time.localtime()))

    dataDir = "data/"+day
    if(not os.path.exists(dataDir)):
        os.makedirs(dataDir)


    fShop = codecs.open( dataDir+"/shops.txt", "a", "utf-8")
    fShopCat = codecs.open( dataDir+"/shopCats.txt", "a", "utf-8")
    fShopItem = codecs.open( dataDir+"/shopItems.txt", "a", "utf-8")


    def parse(self, response):
        s = "https://shopsearch.taobao.com/search?app=shopsearch&q=%E9%9F%A9%E5%9B%BD%E4%BB%A3%E8%B4%AD&imgfile=&js=1&stats_click=search_radio_all%3A1&initiative_id=staobaoz_"+ str(time.strftime("%Y%m%d", time.localtime()) +"&ie=utf8&sort=credit-desc" );
        for i in range(0,1):
            url = s + "&s="+str(20*i)
            logging.debug("--STEP 1----list shop--url-----"+url)
            if(i>0):
                self.fShop.flush()
            yield Request(url=url,callback=self.shopListParse)
        pass

    # shop list
    def shopListParse(self, response):
        body=response.body.decode("utf-8","ignore")
        rr = re.compile(r'{"uid":"(.*?)","title":"(.*?)","nick":".*?","provcity":"(.*?)","totalsold":(.*?),"procnt":.*?,"encodeNick":".*?","goodratePercent":"(.*?)","shopUrl":"//shop(.*?).taobao.com"')
        shopInfoList = rr.findall(body)
        for j in range(0, 1): #todo len(shopInfoList)
            shopId = shopInfoList[j][5]
            url1="https://shop"+str(shopId)+".taobao.com/search.htm?search=y"
            logging.debug('--STEP 2-----shop url----'+url1)
            self.fShop.write(",".join(shopInfoList[j])+"\n")
            if(j>0):
                self.fShopCat.flush()
            yield Request(url=url1,callback=self.shopParse,meta={'shopId':shopId})
        pass



    # shop
    def shopParse(self, response):
        body=response.body.decode("gbk","ignore")
        shopId = response.meta['shopId']
        # self.fShop.write(",".join(shopInfo)+"\n")
        catSelectors = response.xpath('//a[@class="cat-name fst-cat-name"]/text()')
        rsList = []
        for j in range(0,len(catSelectors)):
            catStr = catSelectors[j].extract()
            if(self.filterCat(catStr)):
                continue
            rsList.append(shopId +','+ catStr)
        self.fShopCat.write("\n".join(rsList))
        itemListSuffix = response.css("#J_ShopAsynSearchURL").xpath('@value').extract()
        shopUrl = "https://shop"+str(shopId)+".taobao.com"
        if(len(itemListSuffix) > 0):
            for i in range(1,30): #todo 30
                url = shopUrl + itemListSuffix[0] + "&pageNo="+str(i)
                logging.debug("---STEP 3-----shopParse-----"+url)
                if(i>0):
                    self.fShopItem.flush()
                yield Request(url=url,callback=self.shopItemsParse,meta={'shopId':shopId})
            pass
        else:
            logging.warn("--unmach---:"+ shopUrl)

    def shopItemsParse(self, response):
        body=response.body.decode("gbk","ignore")
        shopId = response.meta['shopId']
        itemList = response.xpath('//dl[contains(@class, "item")]')
        for i in range(0,len(itemList)):
            item = itemList[i]
            itemId= str(item.xpath('@data-id').extract_first()).replace("\\\"","")
            itemName = str(item.xpath('dd/a/text()').extract_first()).strip()
            cPrice = item.xpath('dd/div/div[@class=\'\\"cprice-area\\"\']/span[@class=\'\\"c-price\\"\']/text()').extract_first()
            sPrice = item.xpath('dd/div/div[@class=\'\\"sprice-area\\"\']/span[@class=\'\\"s-price\\"\']/text()').extract_first()
            saleNum = item.xpath('dd/div/div[@class=\'\\"sale-area\\"\']/span[@class=\'\\"sale-num\\"\']/text()').extract_first()
            url = "https://item.taobao.com/item.htm?id=" + itemId
            logging.debug("---STEP 4-----shopItemsParse-----"+url + itemId + itemName)
            yield Request(url= url,callback=self.shopItemDetailParse,meta={'shopId':shopId ,'itemId':itemId,'itemName':itemName,'cPrice':cPrice,'sPrice':sPrice,'saleNum':saleNum})
        pass

    def shopItemDetailParse(self, response):
        body=response.body.decode("gbk","ignore")
        shopId = response.meta['shopId']
        itemId = response.meta['itemId']
        itemName = response.meta['itemName']
        cPrice = response.meta['cPrice']
        sPrice = response.meta['sPrice']
        saleNum = response.meta['saleNum']

        detailCommonUrls = re.compile(r'data-commonApi = "(.*?)"').findall(body)
        if(len(detailCommonUrls) >0):
            detailCommonUrl = detailCommonUrls[0]
            detailCommonUrl.replace("&amp;","&")
            url = "https:"+detailCommonUrl.replace("&amp;","&")
            logging.debug("---STEP 5-----shopItemDetailParse-----"+url)
            yield Request(url= url,callback=self.shopItemSummaryParse,meta={'shopId':shopId ,'itemId':itemId,'itemName':itemName,'cPrice':cPrice,'sPrice':sPrice,'saleNum':saleNum})

    def shopItemSummaryParse(self, response):
        body=response.body.decode("gbk","ignore")
        shopId = self.getDefaultValue(response.meta['shopId'],'0')
        itemId = self.getDefaultValue(str(response.meta['itemId']),'0')
        itemName = self.getDefaultValue(response.meta['itemName'],'0')
        cPrice = self.getDefaultValue(response.meta['cPrice'],'0')
        sPrice = self.getDefaultValue(response.meta['sPrice'],'0')
        saleNum = self.getDefaultValue(response.meta['saleNum'],'0')
        totalCom = self.getArrayResult(re.compile(r'"total":(.*?),').findall(body))
        goodCom = self.getArrayResult(re.compile(r'"good":(.*?),').findall(body))
        normalCom = self.getArrayResult(re.compile(r'"normal":(.*?),').findall(body))
        badCom = self.getArrayResult(re.compile(r'"bad":(.*?),').findall(body))
        additionalCom = self.getArrayResult(re.compile(r'"additional":(.*?),').findall(body))
        picCom = self.getArrayResult(re.compile(r'"pic":(.*?),').findall(body))
        infos = [shopId,itemId,itemName,cPrice,sPrice,saleNum,totalCom,goodCom,normalCom,badCom,additionalCom,picCom]
        try:
            self.fShopItem.write(",".join(infos)+"\n")
        except:
            logging.debug(infos)


    def filterCat(self,cat):
        if(re.compile(r'\s').findall(cat) or str(cat).__contains__("所有宝贝")):
            return True
        else:
            return False

    def getArrayResult(self,m):
        if(m== None or len(m) == 0):
            return "0"
        else:
            return m[0]


    def getDefaultValue(self,i,d):
        if(i == None):
            return d
        else:
            return i


