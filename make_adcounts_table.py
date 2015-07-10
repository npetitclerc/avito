""" Script to shuffle and remove samples without IsClick field
Random query is easy in SQLite, but very slow for large dataset, so do it once and for all and save as a new table.
"""

import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier, Perceptron, PassiveAggressiveClassifier
from sklearn.metrics import log_loss
import time

engine = create_engine('sqlite:////home/ubuntu/data/avito/db/database.sqlite')
engine2 = create_engine('sqlite:////home/ubuntu/data/avito/db/database2.sqlite')

for irun, chunk in enumerate(pd.read_sql_query("SELECT AdID FROM AdsInfo;", engine, chunksize=10)):
    adids = chunk['AdID']
    #search_temp = pd.read_sql_query("SELECT AdID, count(*) as c_search FROM trainSearchStream WHERE AdID in (" + ",".join(map(str, adids)) + ") GROUP BY AdID;", engine)
    search_temp = pd.read_sql_query("SELECT AdID, count(*) as c_search FROM trainSearchRandom WHERE AdID in (" + ",".join(map(str, adids)) + ") GROUP BY AdID;", engine2)
    #print search_temp.keys()
    chunk = pd.merge(chunk, search_temp, how='left', on=['AdID'])

    #ads_temp = pd.read_sql_query("SELECT AdID FROM AdsInfo;", engine)
    #print ads_temp.keys()
    visit_temp = pd.read_sql_query("SELECT AdID, count(*) as c_visit FROM VisitsStream WHERE AdID in (" + ",".join(map(str, adids)) + ") GROUP BY AdID;", engine)
    #print visit_temp.keys()
    visit_temp['c_ratio_visit'] = visit_temp['c_visit'] / search_temp['c_search'].astype(float)
    chunk = pd.merge(chunk, visit_temp, how='left', on=['AdID'])

    phone_temp = pd.read_sql_query("SELECT AdID, count(*) as c_phone FROM PhoneRequestsStream WHERE AdID in (" + ",".join(map(str, adids)) + ") GROUP BY AdID;", engine)
    #print phone_temp.keys()
    phone_temp['c_ratio_phone'] = phone_temp['c_phone'] / search_temp['c_search'].astype(float)    
    chunk = pd.merge(chunk, phone_temp, how='left', on=['AdID'])
    print chunk
    print "--"
    chunk.to_sql('AdCounts', engine2, flavor='sqlite', if_exists='append', index=False)
