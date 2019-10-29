#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Oct 28 22:31:03 2019

@author: anchit
"""

import happybase
import math

connection = happybase.Connection('0.0.0.0', port=9090)
table = connection.table('articles')

user_family='user_cf'
user_pref='username_'

article_family='article_cf'
article_pref='article_'

cf1='cf1'
count_loc=cf1+':count'
content_loc=cf1+':content'



user_ids = ['Monkbot',\
               'TomReding',\
               'BrownHairedGirl',\
               'CeliaHomeford',\
               'InternetArchiveBot',\
               'Chess',\
               'Theoldkinderhook',\
               'TreyHarris',\
               'GraemeBartlett',\
               'Serols',\
               'Shellwood',\
               'PedjaNbg',\
               'Jeronimo',\
               'CAPTAINRAJU',\
               'Carlossuarez46']

user_ids=[user_pref+x for x in user_ids]


article_ids = ['article_5310', 'article_4173', 'article_3817',\
               'article_2279', 'article_2275', 'article_2273',\
               'article_2269', 'article_2264','article_2262',\
               'article_2302', 'article_963', 'article_958',\
               'article_1991','article_1988', 'article_1986',\
               'article_1985', 'article_1984', 'article_1979']

test_user=user_ids[2]

'''
get articles that these users contributed to
data=dict(table.rows(user_ids))
article_ids=[]
for key in data:
    for col in data[key]:
        if str(col).find('article_')>=0:
            article_ids.append(str(col))
print(len(set(article_ids)))
'''

print('Reading Hbase data')
rows=dict(table.rows(user_ids+article_ids))

def get_article_vec(d):
    global article_family
    vec={}
    for key in d:
        k=str(key)
        if k.find(article_family)>=0:
            vec[ k[k.find(article_family)+len(article_family)+1:] ]=int(d[key])
    #print(vec)
    return vec


def get_user_vec(d):
    global user_family
    vec={}
    for key in d:
        k=str(key)
        if k.find(user_family)>=0:
            vec[ k[k.find(user_family)+len(user_family)+1:] ]=int(d[key])
    #print(vec)
    return vec
    
def compute_sim(v1,v2):
    sim=0
    s1=s2=0
    
    for key in v1:
        val=int(v1[key])
        s1=s1+pow(val,2)
        if key in v2:
            sim = sim + val*int(v2[key])
            
    for key in v2:
        val=int(v2[key])
        s2=s2+pow(val,2)
    
    return sim/math.sqrt(s1*s2)
        

print('\n\n\n')
print('Username rowkey : ' + test_user)
print('==========================================\n\n\n')


print('BQ1: Articles user has contributed to : ')
print('==========================================')

ids=[]
for col in rows[test_user.encode()]:
    i=str(col).find('article_')
    if i>=0:
        art_id=str(col)[i:]
        art_id=art_id[:-1]
        ids.append(art_id.encode())
        print(art_id.encode())
    
print('\n\nGetting titles:\n\n')
temp=dict(table.rows(ids,columns=[b'cf1:title']))
for t in temp:
    print(str(t) + ' ' + str(temp[t][b'cf1:title']))

print('\n\n\n')



print('Computing article similarites..... ' )
print('==========================================')
best_article=''
max_sim=0
for x in article_ids:
   user_vec=get_user_vec(rows[test_user.encode()])
   article_vec=get_article_vec(rows[x.encode()])
   sim=compute_sim(user_vec,article_vec)
   if sim>=max_sim:
       best_article=x
       max_sim=sim
   print(sim)
print('\n\n\n')


print('BQ2: Article recommended :' )
print('==========================================')
print('Title:')
print(rows[best_article.encode()][b'cf1:title'])
print('\n\n')
print(rows[best_article.encode()][b'cf1:content'])
print('\n\n\n')


print('BQ3: Other users who have contributed to this article:')
print('==========================================')
for col in rows[best_article.encode()]:
    i=str(col).find('username_')
    if i>=0:
        print(str(col)[i:])
        
print('\n\n\n')


print('BQ4: Most active users:')
print('==========================================')

cnts={}
def count_comp(x):
    global rows
    global cnts
    cnt=0
    for col in rows[x.encode()]:
        i=str(col).find('article_')
        if i>=0:
            cnt=cnt+1
    cnts[x]=cnt
    return cnt,x

user_ids=sorted(user_ids,key=count_comp)
user_ids.reverse()
for x in user_ids[0:3]:
    print(x + '   ' + str(cnts[x]))


print('\n\n\n')

