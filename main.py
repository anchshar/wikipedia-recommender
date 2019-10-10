'''import pyspark
from pyspark import SparkContext
sc =SparkContext()

nums= sc.parallelize([1,2,3,4])
nums.take(1)
squared = nums.map(lambda x: x*x).collect()
for num in squared:
    print('%i ' % (num))
'''


import happybase
connection = happybase.Connection('0.0.0.0', port=9090)


table = connection.table('articles')

#table.put(b'test-key2',{b'cf1:col':b'bam!'})

rows = table.rows([b'test-key',b'test-key2'])
for key, data in rows:
    print(key, data)

