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

chunk_size = 100000
engine = create_engine('sqlite:////home/ubuntu/data/avito/db/database.sqlite')
engine2 = create_engine('sqlite:////home/ubuntu/data/avito/db/database2.sqlite')

for irun, chunk in enumerate(pd.read_sql_query("SELECT UserID FROM UserInfo;", engine, chunksize=chunk_size)):
    sids = chunk['SearchID']
    search_temp = pd.read_sql_query("SELECT SearchID, count(*) as c_search FROM trainSearchStream WHERE SearchID in (" + ",".join(map(str, sids)) + ") GROUP BY SearchID;", engine)
    chunk = pd.merge(chunk, search_temp, how='left', on=['SearchID'])

    #TODO add testStream counts
    
    searchInfo_temp = pd.read_sql_query("SELECT SearchID, UserID, count(*) as uc_search FROM SearchInfo WHERE SearchID in (" + ",".join(map(str, sids)) + ") GROUP BY UserID;", engine)
    userids = chunk['UserID']
    del searchInfo_temp['UserID']
    chunk = pd.merge(chunk, searchInfo_temp, how='left', on=['SearchID'])

    
    #search_temp = pd.read_sql_query("SELECT SearchID, count(*) as sc_search FROM trainSearchStream GROUP BY SearchID;", engine)
    #search_temp = pd.read_sql_query("SELECT AdID, count(*) as c_search FROM trainSearchRandom WHERE AdID in (" + ",".join(map(str, adids)) + ") GROUP BY AdID;", engine2)
    #print search_temp.keys()
    #chunk = pd.merge(chunk, search_temp, how='left', on=['UserID'])

    #ads_temp = pd.read_sql_query("SELECT AdID FROM AdsInfo;", engine)
    #print ads_temp.keys()
    
    #visit_temp = pd.read_sql_query("SELECT UserID, count(*) as uc_visit FROM VisitsStream WHERE UserID in (" + ",".join(map(str, userids)) + ") GROUP BY UserID;", engine)
    #print visit_temp.keys()
    #chunk = pd.merge(chunk, visit_temp, how='left', on=['UserID'])

    #phone_temp = pd.read_sql_query("SELECT UserID, count(*) as uc_phone FROM PhoneRequestsStream WHERE UserID in (" + ",".join(map(str, userids)) + ") GROUP BY UserID;", engine)
    #print phone_temp.keys()
    #chunk = pd.merge(chunk, phone_temp, how='left', on=['UserID'])

    #chunk['c_ratio_visit'] = chunk['c_visit'] / chunk['c_search'].astype(float)
    #chunk['c_ratio_phone'] = chunk['c_phone'] / chunk['c_search'].astype(float)    
    chunk = chunk.fillna(0)
    print chunk[:10]
    print "Processed: ", (irun + 1) * chunk_size
    chunk.to_sql('UserCounts', engine2, flavor='sqlite', if_exists='append', index=False)
