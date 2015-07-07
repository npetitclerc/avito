import numpy as np
import pandas as pd
from sqlalchemy import create_engine
import time
import pickle

engine = create_engine('sqlite:////home/ubuntu/data/avito/db/database.sqlite')
submission_file = "submission.csv"
clf_file = "clf_SGD_histctr_pos.pkl"

def make_chunk_features(chunk):
    """ Make all queries to build the chunk's features
    """
    X_test = chunk[['HistCTR', 'Position']]
    output = pd.DataFrame(chunk['TestId'].astype(int), columns=['ID'])
    X_test = X_test.replace('', 0, regex=True) # Replace empty strings by zeros
    return X_test, output

clf = pickle.load(open(clf_file, 'r'))    
output = pd.DataFrame(columns=['ID', 'IsClick'])
all_classes = np.array([0, 1])
t0 = time.time()    
tf = t0
for irun, chunk in enumerate(pd.read_sql_query("SELECT TestId, HistCTR, Position FROM testSearchStream;", engine, chunksize=2000000)):
    # for chunk in pd.read_sql_query("SELECT * FROM trainSearchStream", engine, chunksize=10000):
    ti = time.time()
    print "Query time: ", ti - tf
    X_test_temp, output_temp = make_chunk_features(chunk)   
    tj = time.time()
    print "Make feature time: ", tj - ti
    y_pred = clf.predict_proba(X_test_temp.values.astype(float))
    
    output_temp['IsClick'] = y_pred[:,1]
    output = output.append(output_temp)
    
    tf = time.time()
    print "Training time: ", tj - ti

output.to_csv(submission_file, index=False)    
    
