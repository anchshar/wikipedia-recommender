
import pyspark
import xml.etree.ElementTree as ET
from pyspark import SparkContext
import happybase
import re


sc =SparkContext()
files='file:///Users/anchit/Desktop/test_dir/enwiki-20190920-pages-articles-multistream1.xml-p10p30302'

table_name='articles'

user_family='user_cf'
user_pref='username_'

article_family='article_cf'
article_pref='article_'

cf1='cf1'
title_col='title'
count_loc=cf1+':count'
content_loc=cf1+':content'


## Util functions
def split_str(x):
    x=x[1]
    x=x[x.find('<page>'):]
    return x+'</page>'


def parse_xml(x):
    
    try:
        root=ET.fromstring(x)
    except:
        print(x)
        return None

    id=root.findall('.//id')[0]
    id=ET.tostring(id).decode()
    id=''.join(e for e in id if e.isalnum())
    id=id[2:len(id)-2]

    username=root.findall('.//username')
    if len(username)==0:
        print('ERROR: '+ str(x))
        return None
    username=username[0]
    username=ET.tostring(username).decode()
    username=username[username.find('username>')+9:]
    username=username[0:username.find('</username')]
    username=''.join(e for e in username if e.isalnum())

    text=root.findall('.//text')
    text=[ET.tostring(t).decode() for t in text]
    text=' '.join(text)
    
    title=root.findall('.//title')
    if len(title)==0:
        title=''
    else:
        title=title[0]
        title=ET.tostring(title).decode()
        title=title[title.find('title>')+6:]
        title=title[0:title.find('</title')]
    

    words=re.split(r'\s+',text)
    words=[''.join(e for e in word if e.isalnum()) for word in words]
    
    vec={}
    for word in words:
        if word in vec:
            vec[word]=vec[word]+1
        else:
            vec[word]=1

    text=' '.join(words)
    return {'id':id,'username':username,'text':text,'vec':vec,'title':title}


def write_hbase(x):
    
    global content_loc
    
    global article_family
    global article_pref
    global user_family
    global user_pref
    global content_loc
    global table_name
    global cf1
    global title_col
    
    if x != None:
        print('keys:')
        print(x['id'] + ' ' + x['username'] + ' ' +x['title'])
        id=x['id']
        content=x['text']
        vec=x['vec'].copy()
        username=x['username']
        title=x['title']

        connection = happybase.Connection('0.0.0.0', port=9090)
        table = connection.table(table_name)
        
        #Fetch row from table
        row=table.row(article_pref+id)
        
        #Append contributions
        if content_loc in row:
            content=row[content_loc]+' '+content
        
        #Calculate contrib count for article
        count=1
        if count_loc in row:
            count=str(count+int(row[count_loc]))
        else:
            count=str(count)
        
        # Aggregate article vector
        for word in vec:
            key=article_family+':'+word
            if key in row:
                vec[word]=vec[word]+int(row[key])
    
        #Copy to new vec
        temp={}
        for word in vec:
            temp[article_family+':'+word]=str(vec[word])
        vec=temp

        
        # Put article vector + content + count + contributor
        vec[content_loc]=content
        vec[count_loc]=count
        vec[cf1+':'+user_pref+username]='true'
        vec[cf1+':'+title_col]=title
        
        table.put(article_pref+id,vec)


        #Fetch user row from table
        row=table.row(user_pref+username)

        #Aggregate user vector
        vec=x['vec'].copy()
        for word in vec:
            key=user_family+':'+word
            if key in row:
                vec[word]=vec[word]+int(row[key])

        count=1
        if count_loc in row:
            count=str(count+int(row[count_loc]))
        else:
            count=str(count)

        #write user vector +count
        temp={}
        for word in vec:
            temp[user_family+':'+word]=str(vec[word])
        vec=temp
        vec[count_loc]=count
        vec[cf1+':'+article_pref+id]='true'

        table.put(user_pref+username,vec)




## call and run
file_rdds=sc.newAPIHadoopFile(files, "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                    "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text",
                    conf={"textinputformat.record.delimiter": "</page>"})\
.map(split_str)\
.map(parse_xml)\
.foreach(write_hbase)

