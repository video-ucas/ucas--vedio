import requests
import time
import pymysql
import re
import schedule

def write_to_mysql():
    try:
        cxn = pymysql.connect(host='localhost', user='root', passwd='yuk_wing',db='spider')     #与数据库建立连接
    except :
        print("error!")
        exit( 0 )

    cur = cxn.cursor()									    #获取操作游标
    try:
        cur.execute("CREATE TABLE parities(Local_time VARCHAR(20) COMMENT'爬取时间',USDCNH VARCHAR(8) COMMENT'美元对人民汇率',DINIW  VARCHAR(8) COMMENT'美元指数')")
    except:
        print('table dollar exist!')
    try:
        cur.execute("create table oil(Local_time VARCHAR(20) COMMENT'爬取时间',Wit VARCHAR(20) COMMENT'Wit原油价格',Brent VARCHAR(20) COMMENT'布伦特原油价格')")		    #创建一个oil表，包含记录时间、原油指数两个参数
    except:
        print ('table oil exist!')
    try:
        cur.execute(
            "create table gold(Local_time VARCHAR(20) COMMENT'爬取时间',AUDT VARCHAR(8) COMMENT'每克黄金价格（RMB）')")  # 创建一个oil表，包含记录时间、原油指数两个参数
    except:
        print('table oil exist!')

    print ("数据写入数据库中...")
    value1 = []
    value2 = []
    value3 = []
    try:
        result(value1, value2, value3)  # 调用result函数，获取我们从网页抓取的数据
    except:
        print('No Values')
    try:
        sql1 = "INSERT INTO parities VALUES(%s, %s,%s)"
        cur.execute(sql1, value1)  # 将对应数据传入parities表中
    except:
        print('write to database fail! ')
    try:
        sql2 = "INSERT INTO oil VALUES(%s, %s, %s)"
        cur.execute(sql2, value2)  # 将对应数据传入oil表中
    except:
        print('write to database fail!')
    try:
        sql3 = "INSERT INTO gold VALUES(%s, %s)"
        cur.execute(sql3, value3)  # 将对应数据传入gold表中
    except:
        print('write to database fail!')
    cxn.commit()  # 提交
    cxn.close()										    #关闭数据库连接

def result(value1,value2,value3):
    url1 = 'https://hq.sinajs.cn/rn=1611035967836list=fx_susdcny'#汇率
    url2 = 'http://hq.sinajs.cn/?rn=1417610565584&list=DINIW'#美元指数
    url3 = 'https://info.usd-cny.com/d.js'#原油价格
    url4 = 'https://hq.sinajs.cn/?_=0.4317191140099734&list=gds_AUTD'#黄金

    html1 = requests.get(url1)
    html2 = requests.get(url2)
    html3 = requests.get(url3)
    html4 = requests.get(url4)

    ISOTIMEFORMAT= '%Y-%m-%d %X'#格式化时间
    value1.append(time.strftime(ISOTIMEFORMAT,time.localtime()))
    value2.append(time.strftime(ISOTIMEFORMAT,time.localtime()))
    value3.append(time.strftime(ISOTIMEFORMAT,time.localtime()))

    USDCNY = re.compile('var hq_str_fx_susdcny=".*?,(.*?),.*?";').findall(html1.text)#获取人命币汇率
    DINIW = re.compile('".*?,(.*?),.*?";').findall(html2.text)#美元指数
    info = re.compile('.*?,(.*?),.*?').findall(html3.text)
    wit = info[1]    #wit油价
    brent  = info[8] #布伦特油价
    AUDT = re.compile('"(.*?),.*?').findall(html4.text)#黄金报价

    value1.append(USDCNY[0].encode('utf-8'))
    value1.append(DINIW[0].encode('utf-8'))
    value2.append(wit.encode('utf-8'))
    value2.append(brent.encode('utf-8'))
    value3.append(AUDT[0].encode('utf-8'))

schedule.every(1).minutes.do(write_to_mysql)

while True:
    schedule.run_pending()  # run_pending：运行所有可以运行的任务
