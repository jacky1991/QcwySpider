# -*- coding:UTF-8 -*-

import scrapy
import re
import traceback
import urllib.request
from scrapy.http import Request
from QcwySpider.items import QcwyspiderItem
from QcwySpider.commoncode import *


class QcwySpider(scrapy.Spider):
    logger = logging.getLogger()

    name = "qcwyspider"
    allowed_domains = ['51job.com']
    CommonCode.DEALNUMBER = 0
    CommonCode.ALLNUMBER = 0
    start_urls = []
    # for line in open("customer_zhilian.txt"):
    #     if (line != '' and line.strip() != ''):
    keyword = "饿了么"
    gsmc = urllib.request.quote(urllib.request.quote(keyword.strip()))
    # 全国
    # 公司名搜索
    ssurl1 = 'https://search.51job.com/list/000000,000000,0000,00,9,99,'+gsmc+',1,1.html'
    # 关键字搜索
    # ssurl1 = 'https://search.51job.com/list/020000%252C070400%252C070300,000000,0000,00,9,99,' + gsmc + ',2,1.html'
    print(ssurl1);
    start_urls.append(ssurl1)
    CommonCode.ALLNUMBER = CommonCode.ALLNUMBER + 1
    print("一共客户请求数：" + str(len(start_urls)))


    def parse(self, response):
        CommonCode.DEALNUMBER = CommonCode.DEALNUMBER + 1
        print("处理进度：" + str(CommonCode.DEALNUMBER) + "/" + str(CommonCode.ALLNUMBER))
        self.logger.info("处理进度：" + str(CommonCode.DEALNUMBER) + "/" + str(CommonCode.ALLNUMBER))
        try:
            # 第一页数据
            zw_table = response.xpath('//div[@id="resultList"]/div[@class="el"]')
            # 遍历每个职位
            print("开始处理第1页数据，职位数量："+str(len(zw_table)))
            for i in range(len(zw_table)):
                if (i >= 0):
                    zwmc = zw_table[i].xpath('.//p/span/a[1]/@title').extract()
                    zwmcurl = zw_table[i].xpath('.//p/span/a[1]/@href').extract()
                    gsmc = zw_table[i].xpath('.//span[@class="t2"]//a[1]/@title').extract()
                    zwyx = zw_table[i].xpath('.//span[@class="t4"]//text()').extract()
                    gzdd = zw_table[i].xpath('.//span[@class="t3"]//text()').extract()
                    fbrq =  zw_table[i].xpath('.//span[@class="t5"]//text()').extract()
                    #
                    item = QcwyspiderItem()
                    item['zwmc'] = zwmc[0]
                    item['gsmc'] = gsmc[0]
                    item['zwyx'] = zwyx[0]
                    item['gzdd'] = gzdd[0]
                    item['zwmcurl'] = zwmcurl[0]
                    item['fbrq'] = fbrq[0]
                    item['fkl'] = ''
                    # print(item)
                    yield Request(item['zwmcurl'], meta={'item': item}, callback=self.parse_item_info)
        except Exception as err:
            print(err)
            self.logger.info("处理第一页职位列表异常：" + response.url + str(err))
            CommonCode.insertErrorLog("处理第一页职位列表出错：" + response.url, str(err))
        # 获取分页信息，并查询下一页
        try:
            # 获取总页数
            pattern = re.compile(r'\d+')
            countNumber = int(pattern.match(response.xpath('//div[@class="dw_tlc"]//div[@class="rt"]/text()').extract()[0].strip(),1).group(0))
            if (countNumber > 0):
                theUrl = response.url
                countPage = int(pattern.match(response.xpath('//div[@class="p_in"]/span[1]/text()').extract()[0], 1).group(0))
                print("本次抓取职位总数：" + str(countNumber)+",总共"+str(countPage)+"页")
                for m in range(countPage):
                    if (m > 0):
                        nexturl = theUrl.split(',1,')[0] + ',1,' + str(m + 1)+'.html'
                        # print(nexturl)
                        yield Request(nexturl,meta={"pagenum":m+1}, callback=self.parse_item)
        except Exception as err:
            print(err)
            traceback.print_exc()
            self.logger.info("获取下一页异常：" + response.url + str(err))
            CommonCode.insertErrorLog("获取下一页出错：" + response.url, str(err))

    # 处理一页一页的数据
    def parse_item(self, response):
        try:
            pagenum = response.meta["pagenum"]
            # 职位信息table
            zw_table = response.xpath('//div[@id="resultList"]/div[@class="el"]')
            print("开始处理第" + str(pagenum) + "页数据，职位数量：" + str(len(zw_table)))
            print(response.url)
            # 遍历每个职位
            for i in range(len(zw_table)):
                if (i >= 0):
                    zwmc = zw_table[i].xpath('.//p/span/a[1]/@title').extract()
                    zwmcurl = zw_table[i].xpath('.//p/span/a[1]/@href').extract()
                    gsmc = zw_table[i].xpath('.//span[@class="t2"]//a[1]/@title').extract()
                    zwyx = zw_table[i].xpath('.//span[@class="t4"]//text()').extract()
                    gzdd = zw_table[i].xpath('.//span[@class="t3"]//text()').extract()
                    fbrq = zw_table[i].xpath('.//span[@class="t5"]//text()').extract()
                    #
                    item = QcwyspiderItem()
                    item['zwmc'] = zwmc[0]
                    item['gsmc'] = gsmc[0]
                    item['zwyx'] = zwyx[0]
                    item['gzdd'] = gzdd[0]
                    item['zwmcurl'] = zwmcurl[0]
                    item['fbrq'] = fbrq[0]
                    item['fkl'] = ''
                    yield Request(item['zwmcurl'], meta={'item': item}, callback=self.parse_item_info)
        except Exception as err:
            print(err)
            traceback.print_exc()
            self.logger.info("处理下一页职位列表异常：" + response.url + str(err))
            CommonCode.insertErrorLog("处理下一页职位列表出错：" + response.url, str(err))

    # 处理一个职位连接里的详情
    def parse_item_info(self, response):
        try:
            item = response.meta['item']
            # 福利
            fl = ''
            flarray = response.xpath('//div[@class="jtag inbox"]/p/span/text()').extract()
            for i in range(len(flarray)):
                if (i == 0):
                    fl = fl + flarray[i]
                else:
                    fl = fl + ',' + flarray[i]
            # print(fl)
            gzjy = response.xpath('//div[@class="jtag inbox"]//span[@class="sp4"][1]/text()').extract()[0]
            zdxl = response.xpath('//div[@class="jtag inbox"]//span[@class="sp4"][2]/text()').extract()[0]
            zprs = response.xpath('//div[@class="jtag inbox"]//span[@class="sp4"][3]/text()').extract()[0][1:]
            zwlb = response.xpath('//div[@class="bmsg job_msg inbox"]//div[@class="mt10"]//span[2]/text()').extract()[0]

            zwmss = response.xpath('//div[@class="bmsg job_msg inbox"]//text()')
            zwms = ''
            if (len(zwmss) > 0):
                for i in range(1,len(flarray)):
                    zwms=zwms+zwmss[i].extract()+"<br>"

            if ('工作地址：' in zwms):
                zwms = zwms[0:int(zwms.index('工作地址：'))]
            item['fl'] = fl
            item['zwlb'] = zwlb
            item['gzjy'] = gzjy
            item['zdxl'] = zdxl
            item['zprs'] = zprs
            item['zwms'] = zwms
            # print(item)
            self.logger.info("解析成功：" + item['gsmc'] + "-" + item['zwmc'])
            yield item
        except Exception as err:
            print(err)
            traceback.print_exc()
            self.logger.info("处理职位详情异常：" + response.url + str(err))
            CommonCode.insertErrorLog("处理职位详情出错：" + response.url, str(err))
