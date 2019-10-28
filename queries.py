#queries to retrieve data for answering our BQ's

import pandas as pd
import happybase


#connection = happybase.Connection('0.0.0.0', port=9090)
#table = connection.table('articles')

# list of subset of article ids
article_ids = []

# list of subset of user ids
user_ids = []

print(articles_ids)
print(users_ids)

# retrieving data for all articles in article list as dictionary
articles_info = dict(table.rows(article_ids))
art_df = pd.DataFrame.from_dict(art, orient = "index", columns = ["values"]) # would require change

# retrieving data for all users in user list
users_info = dict(table.rows(user_ids))
art_df = pd.DataFrame.from_dict(art, orient = "index", columns = ["values"]) # would require change

# easy BQ's

#1 identify users that have contributed to a particular article, say article254
row_1 = table.row(b'article254', columns=[b'users_cf'])

#2 identify the number of articles a user has contributed to, say user5
row_2 = table.row(b'user5', columns=[b'article_cf'])
for key,data in row_2:
    for k,v in data.items():
    print (k, len(list(filter(None, v))))

#3 ranking - Aviral, Hunter 
# consider you have any data frame and do a ranking for articles and users - we can modify that accordingly

#4 other business questions

#5 network - Shreya, tomorrow's meeting




