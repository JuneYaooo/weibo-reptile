#!/usr/bin/python
# -*- coding: UTF-8 -*-

## 创建新表for新浪爬虫内容（新浪全文）
import pymysql

# 打开数据库连接
db = pymysql.connect("localhost", "root", "1234", "test", charset='utf8' )  #自己改一下这里的数据库信息，改成自己的，比如用户名root 密码1234 数据库test

# 使用cursor()方法获取操作游标 
cursor = db.cursor()

# 如果数据表已经存在使用 execute() 方法删除表。
cursor.execute("DROP TABLE IF EXISTS weibo_ysxw_new") 

# 创建数据表SQL语句
sql = """CREATE TABLE weibo_ysxw_new (
         text  TEXT,
         attitudes INT,  
         comments INT,
         reposts INT ,
         contentid  varchar(32) unique)   """

cursor.execute(sql)

# 关闭数据库连接
db.close()
print('successful')
