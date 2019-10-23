

import happybase
connection = happybase.Connection('0.0.0.0', port=9090)


table = connection.table('articles')

#table.put(b'test-key3',{b'cf1:col':b'bam!'})

rows = table.rows(['id11712id'])
for key, data in rows:
    print(key, data)

