#!/usr/bin/env python3
#-*- coding:utf-8 -*-



import requests
import json
import sqlite3
import datetime


#导入配置
import sys
sys.path.append('..')
import color_setting as cs
c_loggly_name = cs.loggly_name
c_username = cs.username
c_password = cs.password
c_authorization = cs.authorization



class loggly_info(object):

    def __init__(self, loggly_name=None, username=None, password=None, q='*', fromtime='-10m', untiltime='now', size='30',authorization=None):
        self.loggly_name = loggly_name
        self.username = username
        self.password = password
        self.query = q
        self.fromtime =fromtime
        self.untiltime = untiltime
        self.size = size
        self.authorization = authorization

    #下载初始网页，传入数据参数
    def getRsid(self):
        url = 'https://{}.loggly.com/apiv2/search?q={}&from={}&until={}&size={}'.format(
            self.loggly_name, self.query, self.fromtime, self.untiltime, self.size)
        #print(url)
        headers = {
            'authorization':self.authorization,
            'user - agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.98 Safari/537.36 LBBROWSER'
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

    #下载nextpage
    def download_loggly_info(self, Rsid):
        url = 'http://{}.loggly.com/apiv2/events?rsid={}'.format(
            self.loggly_name,Rsid)
        print(url)
        headers = {
            'authorization': self.authorization,
            'user - agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.98 Safari/537.36 LBBROWSER'
        }
        params = {
            'username': self.username,
            'password': self.password
        }
        response = requests.get(url, params=params, headers=headers)
        html = response.text
        #print(html)
        event_count = json.loads(html)['total_events']        
        return html

    #解析数据
    def parse_loggly(self,html):
        data = json.loads(html)['events']
        #print(data_next_url)
        #print(len(data))
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

    def __init__(self):
        self.loggly_name = c_loggly_name
        self.username = c_username
        self.password = c_password
        self.query = 'json.type:"game_start"'
        self.fromtime = '-24h'
        self.untiltime = 'now'
        self.size = '1000' #此处最大值为1000
        self.authorization = c_authorization
        self.table = 'game_start'
    
    #根据特定的数据写入数据库
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
        print('本次插入{}条数据'.format(n))
        return n
    
if __name__ == '__main__':
    loggly = color_loggly()
    rsid = loggly.getRsid()
    html = loggly.download_loggly_info(rsid)
    data_list = loggly.parse_loggly(html)
    loggly.insert_sql(data_list)

    # starttime = datetime.datetime.now()
    # golf = golf_loggly_reconnect()
    # html = golf.download_loggly()
    # data_list,data_next_url = golf.parse_loggly(html)
    # num = golf.parse_loggly_reconnect(data_list,data_next_url)
    # endtime = datetime.datetime.now()
    # print('本次记录了{}条数据，共运行了{}秒'.format(num,(endtime - starttime).seconds))
