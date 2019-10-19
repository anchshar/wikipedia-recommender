
import pyspark
import xml.etree.ElementTree as ET
from pyspark import SparkContext
import happybase


sc =SparkContext()
files='file:///Users/anchit/Desktop/test_dir/enwiki-20190920-pages-articles-multistream1.xml-p10p30302'


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

    text=root.findall('.//text')
    text=[ET.tostring(t).decode() for t in text]
    text=' '.join(text)
    text=''.join(e for e in text if e.isalnum())
    return id,text


def write_hbase(x):
    
    if x != None:
        print('key')
        print(x[0])
        connection = happybase.Connection('0.0.0.0', port=9090)
        table = connection.table('articles')
        #table.put(b'spark',{b'cf1:col':str(x)})
        table.put(x[0],{b'cf1:col':x[1]})


## call and run
file_rdds=sc.newAPIHadoopFile(files, "org.apache.hadoop.mapreduce.lib.input.TextInputFormat",
                    "org.apache.hadoop.io.LongWritable", "org.apache.hadoop.io.Text",
                    conf={"textinputformat.record.delimiter": "</page>"})\
.map(split_str)\
.map(parse_xml)\
.foreach(write_hbase)

