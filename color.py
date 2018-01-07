#!/usr/bin/env python3
#-*- coding:utf-8 -*-



import requests
import json
import sqlite3
import datetime
import time
import random
from retrying import retry
import base64
import re


#导入配置
import sys
sys.path.append('..')
import color_setting as cs
c_loggly_name = cs.loggly_name
c_username = cs.username
c_password = cs.password
c_timeinterval = cs.timeinterval

#增加每步时间统计
def cost_time(func):
    def wrapper(*args, **kw):
        #print('run【{}】'.format(func.__name__))
        start = '{},【{}】begin!'.format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),(func.__name__))
        print(start)
        a = datetime.datetime.now()
        fn = func(*args, **kw)
        b = datetime.datetime.now()
        end = '{},【{}】end,本次共运行了{}秒!'.format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),(func.__name__),(b - a).seconds)
        with open('C:\workspace\loggly\colordb\detailed_log.txt','a+',encoding='utf-8') as f:
            f.write('{}\n{}\n'.format(start,end))
        print(end)
        return fn
    return wrapper




class loggly_info(object):

    @cost_time
    def __init__(self, loggly_name=None, username=None, password=None, q='*', fromtime='-10m', untiltime='now', size='30'):
        self.loggly_name = loggly_name
        self.username = username
        self.password = password
        self.query = q
        self.fromtime =fromtime
        self.untiltime = untiltime
        self.size = size
        
    def authorization(self):
        info = '{}:{}'.format(self.username,self.password).encode('utf-8')
        authorization = str(base64.b64encode(info))
        authorization = re.findall("b'(.*?)'",authorization)[0]
        authorization = 'Basic {}'.format(authorization)
        return authorization

    #获取rsid
    @cost_time
    @retry
    def getRsid(self):
        url = 'https://{}.loggly.com/apiv2/search?q={}&from={}&until={}&size={}'.format(
            self.loggly_name, self.query, self.fromtime, self.untiltime, self.size)
        #print(url)
        headers = {
            'authorization':self.authorization()
            #'user - agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.98 Safari/537.36 LBBROWSER'
        }
        params = {
            'username':self.username,
            'password':self.password,
        }
        response = requests.get(url,params=params,headers=headers)
        html = response.text
        #print(response.url)
        Rsid = json.loads(html)['rsid']['id']
        #print(Rsid)
        return Rsid

    #下载
    @cost_time
    @retry(wait_random_min=1000, wait_random_max=6000,stop_max_attempt_number=100)
    def download_loggly_info(self, Rsid):
        url = 'http://{}.loggly.com/apiv2/events?rsid={}'.format(
            self.loggly_name,Rsid)
        #print(url)
        headers = {
            'authorization': self.authorization()
            #'user - agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.98 Safari/537.36 LBBROWSER'
        }
        params = {
            'username': self.username,
            'password': self.password
        }

        response = requests.get(url, params=params, headers=headers,timeout=10)
        html = response.text
        event_count = json.loads(html)['total_events']
        if event_count > int(self.size):
            error = '{0},【error】此处数据条数超过{2},实际{1}条'.format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),event_count,self.size)
            print(error)
            with open('C:\workspace\loggly\colordb\log.txt','a+',encoding='utf-8') as f:
                f.write(error)
                f.write('\n')
            #sys.exit(0)
        return html

    #解析数据
    @cost_time
    def parse_loggly(self,html):
        data = json.loads(html)['events']
        data_list = []
        for i in range(0,len(data)):
            one_data = data[i]['event']['json']
            #data = json.loads(data)
            #print(one_data)
            # for k in one_data.keys():
            #     print("one_data['{}'],".format(k))
            # break
            data_list.append(one_data)
        return data_list


#特定的数据表type
class color_loggly(loggly_info):

    #@cost_time
    def __init__(self):
        self.loggly_name = c_loggly_name
        self.username = c_username
        self.password = c_password
        self.query = 'json.type:"game_start"'
        self.fromtime = c_timeinterval
        self.untiltime = 'now'
        self.size = '5000' #此处最大值为5000
        self.table = 'game_start'
    
    #根据特定的数据写入数据库
    #@cost_time
    def insert_sql(self,data_list):
        conn = sqlite3.connect(
                    'C:\workspace\loggly\colordb\{}.db'.format(self.loggly_name), timeout=5)
        cs = conn.cursor()
        try:
            #new_user BLOB 去掉
            cs.execute('''create table {} 
                        (gameVersion text,
                        language text,
                        new_user text,
                        biAppName text,
                        logId text unique,
                        utcTime text,
                        userId text,
                        buildEnv text,
                        clientId text,
                        nation text,
                        clientTime text,
                        platform text,
                        buildNo text,
                        version text,
                        completed_num text,
                        valueToSum text,
                        play_times text,
                        type text,
                        groupId text,
                        iap_status text
                        );'''.format(self.table))
        except:
            pass
        n = 0
        for one_data in data_list:
            #print(one_data)
            # for k in one_data.keys():
            #     print('one_data.get["{}",default="null"]'.format(k))
            # break
            
            try:
                conn.execute('insert into {} VALUES ("{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}")'.format(self.table, one_data['gameVersion'], one_data['language'],one_data.setdefault('new_user',"null"), one_data['biAppName'], one_data['logId'], one_data.setdefault('utcTime',"null"), one_data.setdefault('userId',"null"), one_data['buildEnv'], one_data['clientId'], one_data['nation'], one_data['clientTime'], one_data['platform'], one_data['buildNo'], one_data['version'], one_data['completed_num'], one_data['valueToSum'], one_data['play_times'], one_data['type'], one_data['groupId'], one_data['iap_status']))
                n += 1
                #print(n)
            except:
                #print('重复')
                pass
        
        conn.commit()
        conn.close()
        with open('C:\workspace\loggly\colordb\log.txt','a+',encoding='utf-8') as f:
            f.write('{},本次插入{}条数据'.format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),n))
            f.write('\n')
        print('{},本次插入{}条数据'.format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),n))
        return n
    
if __name__ == '__main__':
    n =0
    while True:
        n += 1
        starttime = datetime.datetime.now()
        loggly = color_loggly()
        rsid = loggly.getRsid()
        html = loggly.download_loggly_info(rsid)
        data_list = loggly.parse_loggly(html)
        num = loggly.insert_sql(data_list)
        endtime = datetime.datetime.now()
        print('第{}次运行，本次记录了{}条数据，共运行了{}秒！'.format(n,num,(endtime - starttime).seconds))
        time.sleep(60)
        

