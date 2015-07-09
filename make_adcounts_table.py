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

ads_temp = pd.read_sql_query("SELECT AdID FROM AdInfo;", engine)
adids = ads_temp['AdID'].unique()

visit_temp = pd.read_sql_query("SELECT AdID, count(*) as c_visit FROM VisitsStream GROUP BY AdID;", engine)
phone_temp = pd.read_sql_query("SELECT AdID, count(*) as c_phone FROM PhoneRequestsStream  GROUP BY AdID;", engine)
search_temp = pd.read_sql_query("SELECT AdID, count(*) as c_search FROM SearchStream GROUP BY AdID;", engine)

visit_temp['c_ratio_visit'] = visit_temp['c_visit'] / search_temp['c_search'].astype(float)
phone_temp['c_ratio_phone'] = phone_temp['c_phone'] / search_temp['c_search'].astype(float)    

ads_train_temp = pd.merge(ads_temp, search_temp, how='left', on=['AdID'])
ads_temp = pd.merge(ads_temp, visit_temp, how='left', on=['AdID'])
ads_train_temp = pd.merge(ads_temp, phone_temp, how='left', on=['AdID'])

ads_temp.to_sql('AdCounts', engine2, flavor='sqlite', if_exists='append', index=False)
