#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 29 16:46:26 2019

@author: anchit
"""

import happybase
import math
import pandas as pd

connection = happybase.Connection('0.0.0.0', port=9090)
table = connection.table('articles')

user_family='user_cf'
user_pref='username_'

article_family='article_cf'
article_pref='article_'

cf1='cf1'
count_loc=cf1+':count'
content_loc=cf1+':content'



user_ids = ['BrownHairedGirl']
               

user_ids=[user_pref+x for x in user_ids]


article_ids = ['article_5310', 'article_4173', 'article_3817',\
               'article_2279', 'article_2275', 'article_2273',\
               'article_2269', 'article_2264','article_2262',\
               'article_2302', 'article_963', 'article_958',\
               'article_1991','article_1988', 'article_1986',\
               'article_1985', 'article_1984', 'article_1979']

print('Reading Hbase data')
rows=dict(table.rows(user_ids))
users=[]
arts=[]
df=pd.DataFrame()
for x in rows:
    user=x
    for col in rows[x]:
        i=str(col).find('article_')
        if i>=0:
            art_id=str(col)[i:]
            art_id=art_id[:-1]
            arts.append(art_id.encode())
            users.append(user)

titles=[]
rows=dict(table.rows(arts,columns=['cf1:title']))
for x in arts:
    titles.append(rows[x][b'cf1:title'])
       
df = pd.DataFrame(list(zip(users, titles)), columns =['users', 'article']) 
print(df.head())
df.to_csv('demo.csv',index=False)
    

