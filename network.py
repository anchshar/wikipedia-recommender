#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Oct 29 16:46:26 2019

"""

import networkx as nx
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

df = pd.read_csv('/Users/datar010/Documents/Wiki/demo.csv')
#df = pd.DataFrame({ 'articles':['D', 'A', 'B', 'C','A'], 'users':['1', '4', '1', '5','3']})

chars = df2 = pd.DataFrame()
chars['ID'] = df['users']
chars['type'] = "users"
df2['ID'] = df['article']
df2['type'] = "article"

chars.head()

G = nx.from_pandas_edgelist(df, 'users', 'article', create_using=nx.DiGraph() )
#nx.draw(G, with_labels=True, font_weight = 'bold' node_size=1500, alpha=0.3, arrows=True)

G.nodes()

chars = chars.set_index('ID')
chars = chars.reindex(G.nodes())

chars['type']=pd.Categorical(chars['type'])
chars['type'].cat.codes

#nx.draw(G, with_labels=True, node_color=chars['type'].cat.codes, cmap=plt.cm.Set1, node_size=2000)

# larger figure size
plt.figure(3,figsize=(12,12))
nx.draw(G,font_size = 14, with_labels=True, node_color=chars['type'].cat.codes, cmap=plt.cm.Set1, node_size=2000)

#nx.draw(G, with_labels = True)
plt.savefig("Graph.png", format="PNG")
plt.show()
