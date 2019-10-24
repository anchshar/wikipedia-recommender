
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
article_pref='article'

content_loc='cf1:content'


## Util functions
def split_str(x):
    x=x[1]
    x=x[x.find('<page>'):]
    return x+'</page>'


def parse_xml(x):
    ret=None
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
        return None
    username=username[0]
    username=ET.tostring(username).decode()
    #username=''.join(e for e in id if e.isalnum())
    username=username[8:len(username)-8]

    text=root.findall('.//text')
    text=[ET.tostring(t).decode() for t in text]
    text=' '.join(text)

    words=re.split(r'\s+',text)
    vec={}
    for word in words:
        if word in vec:
            vec[word]=vec[word]+1
        else:
            vec[word]=1

    words=[''.join(e for e in word if e.isalnum()) for word in words]

    text=' '.join(words)
    return {'id':id,'username':username,'text':text,'vec':vec}


def write_hbase(x):
    
    global content_loc
    
    global article_family
    global article_pref
    global user_family
    global user_pref
    global content_loc
    global table_name
    
    if x != None:
        print('key')
        print(x['id'])
        id=x['id']
        content=x['text']
        vec=x['vec'].copy()
        username=x['username']

        connection = happybase.Connection('0.0.0.0', port=9090)
        table = connection.table(table_name)
        
        #Fetch row from table
        row=table.row(article_pref+id)
        
        #Append contributions
        if content_loc in row:
            content=row[content_loc]+' '+content
        
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

        # Put article vector + content
        vec[content_loc]=content
        table.put(article_pref+id,vec)


        #Fetch user row from table
        row=table.row(user_pref+username)

        #Write user vector
        vec=x['vec'].copy()
        for word in vec:
            key=user_family+':'+word
            if key in row:
                vec[word]=vec[word]+int(row[key])

        temp={}
        for word in vec:
            temp[user_family+':'+word]=str(vec[word])
        vec=temp
        
        table.put(user_pref+username,vec)




## call and run
file_rdds=sc.newAPIHadoopFile(files, "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                    "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text",
                    conf={"textinputformat.record.delimiter": "</page>"})\
.map(split_str)\
.map(parse_xml)\
.foreach(write_hbase)

