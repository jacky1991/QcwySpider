import pymysql
import logging

class CommonCode(object):
    ALLNUMBER=0
    DEALNUMBER=0
    EXCEPTIONNUMBER=0
    #收集错误日志，采集中错误的信息都放到此表
    def insertErrorLog(msg,errorStr):
        logger = logging.getLogger()
        try:
            cxn = pymysql.Connect(host='127.0.0.1', user='root', passwd='', db="zhaopin", charset="utf8")
            # cxn = pymysql.Connect(host = '127.0.0.1', user = 'root', passwd = 'sjq54288',db="zhaopin",charset = "utf8")
            # cxn = pymysql.Connect(host = '192.168.72.164', user = 'root', passwd = 'newcapec',db="zhaopin",charset = "utf8")
            #游标
            cur = cxn.cursor()
            sql = "insert into source_qcwy_errlog (msg,err,sj) values (%s,%s,now())"

            cur.execute(sql,[msg,errorStr])

            #关闭
            cur.close()
            cxn.commit()
            cxn.close()
        except Exception as err:
            logger.info("插入错误日志表异常:"+errorStr)
            print("插入错误日志表出错啦。。。")
            print(err)