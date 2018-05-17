# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import scrapy
from QcwySpider.commoncode import *

class QcwyspiderPipeline(scrapy.Item):
    logger = logging.getLogger()

    def database(self, item):
        try:
            cxn = pymysql.Connect(host='127.0.0.1', user='root', passwd='', db="zhaopin", charset="utf8")
            # cxn = pymysql.Connect(host='127.0.0.1', user='root', passwd='sjq54288', db="zhaopin", charset="utf8")
            # cxn = pymysql.Connect(host='192.168.72.164', user='root', passwd='newcapec', db="zhaopin", charset="utf8")
            # 游标
            cur = cxn.cursor()
            sql = "insert into source_qcwy (zwmc,fkl,gsmc,zwyx,gzdd,zwmcurl,fbrq,fl,zwlb," \
                  "gzjy,zdxl,zprs,zwms,cjsj,source,level) " \
                  "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,now(),'前程无忧','1')"
            sql_delete = "delete from source_zhilian where zwmc=%s and gsmc=%s and gzdd=%s"
            self.logger.info("删除语句：" + sql_delete)
            self.logger.info("添加语句：" + sql)
            cur.execute(sql_delete, [item['zwmc'], item['gsmc'], item['gzdd']])
            cur.execute(sql, [item['zwmc'], item['fkl'], item['gsmc'], item['zwyx'], item['gzdd'],
                              item['zwmcurl'], item['fbrq'], item['fl'], item['zwlb'], item['gzjy'], item['zdxl'],
                              item['zprs'], item['zwms']])

            # 关闭
            cur.close()
            cxn.commit()
            cxn.close()
        except Exception as err:
            self.logger.info("保存Item异常" + str(err) + ":" + item['gsmc'] + "-" + item['zwmc'])
            CommonCode.insertErrorLog("保存Item出错：" + item['gsmc'] + "-" + item['zwmc'], str(err))
            print("插入采集职位表出错啦。。。")
            print(err)

    def process_item(self, item, spider):
        self.database(item)
        return item