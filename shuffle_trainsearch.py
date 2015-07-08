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
nrows = 0
for chunk in pd.read_sql_query("SELECT * FROM trainSearchStream WHERE IsClick IN (0,1) ORDER BY RANDOM();", engine, chunksize=2000000):
    #for chunk in pd.read_sql_query("SELECT * FROM trainSearchStream WHERE IsClick IN (0,1) LIMIT 1000;", engine, chunksize=200):
    chunk.to_sql('trainSearchRandom', engine2, flavor='sqlite', if_exists='append', index=False)
    nrows += len(chunk)    
    print nrows
