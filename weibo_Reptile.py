# 抓取微博全文内容

import requests
from urllib.parse import urlencode
from pyquery import PyQuery as pq
import pandas as pd
from sqlalchemy import create_engine
import pymysql

host = 'm.weibo.cn'
base_url = 'https://%s/api/container/getIndex?' % host
user_agent = 'User-Agent: Mozilla/5.0 (iPhone; CPU iPhone OS 9_1 like Mac OS X) AppleWebKit/601.1.46 (KHTML, like Gecko) Version/9.0 Mobile/13B143 Safari/601.1 wechatdevtools/0.7.0 MicroMessenger/6.3.9 Language/zh_CN webview/0'

headers = {
    'Host': host,
    'Referer': 'https://m.weibo.cn/u/2656274875',  #这里是想要爬的微博的ID，自己替换一下，这里以央视新闻为例
    'User-Agent': user_agent
}



# 按页数抓取数据
def get_single_page(page):
    params = {
        'type': 'uid',
        'value': 2058586920,  #随便去网上找了个，但这里需要替换成自己的微博信息
        'containerid': 1076032058586920,  #随便去网上找了个，但这里需要替换成自己的微博信息
        'page': page
    }
    url = base_url + urlencode(params)
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
    except requests.ConnectionError as e:
        print('catch wrong', e.args)


# 解析原页面返回的json数据
def parse_page(json):
    items = json.get('data').get('cards')
    for item in items:
        item = item.get('mblog')
        if item:
            data = {
                'id': item.get('id')
            }
            yield data

# 按内容ID抓取数据
def get_single_content(contentid):
    base_url2='https://m.weibo.cn/statuses/extend?id='
    url2 = base_url2 + contentid
    headers2 = {
    'Host': host,
    'Referer': 'https://m.weibo.cn/status/'+contentid,
    'User-Agent': user_agent}
    print(url2)  #打印出来要爬的这条微博的URL，不想看打印出来的信息可以注释掉，不影响最后写入数据库
    try:
        response = requests.get(url2, headers=headers2)
        if response.status_code == 200:
            return response.json()
    except requests.ConnectionError as e:
        print('catch wrong', e.args)

# 解析全文展开页面返回的json数据
def parse_page_content(json2):
    item2 = json2.get('data')
    if item2:
        data = {
            'text': pq(item2.get("longTextContent")).text(),  # 仅提取内容中的文本
            'attitudes': item2.get('attitudes_count'),
            'comments': item2.get('comments_count'),
            'reposts': item2.get('reposts_count')
        }
        yield data


def exportdata():
       for page in range(20, 25):  # 抓取前十页的数据
        json = get_single_page(page)
        results = parse_page(json)
        db = pymysql.connect("localhost", "root", "1234", "test", charset='utf8' )     # 打开数据库连接
        cursor = db.cursor()         # 使用cursor()方法获取操作游标 
        table = 'weibo_ysxw_new'  #mysql数据库里建的表
        for result in results:
            contentid = result['id']
            json2=get_single_content(contentid)
            results2=parse_page_content(json2)
            for result2 in results2:
                result2['contentid']=contentid
                print(result2)  #把抓取的每一条数据打印出来看看
                data=result2
                keys = ', '.join(data.keys())
                values = ', '.join(['%s'] * len(data))
                sql = 'INSERT INTO {table}({keys}) VALUES({values}) ON DUPLICATE KEY UPDATE '.format(table=table, keys=keys,values=values)  #ON DUPLICATE KEY UPDATE表示当主键存在时，执行更新操作
                #更新
                updata = ', '.join(["{key} = %s".format(key=key) for key in data])   #连接起来实现更新数据
                sql += updata
                try:
                    if cursor.execute(sql, tuple(data.values()) * 2):
                        print("successful")
                        db.commit()
                except:
                    print('failed')
                    db.rollback()
        db.close()



if __name__ == '__main__':
            exportdata()
