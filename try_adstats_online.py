import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from sklearn.naive_bayes import MultinomialNB
from sklearn.linear_model import SGDClassifier, Perceptron, PassiveAggressiveClassifier
from sklearn.metrics import log_loss
import time

engine = create_engine('sqlite:////home/ubuntu/data/avito/db/database.sqlite')
engine2 = create_engine('sqlite:////home/ubuntu/data/avito/db/database2.sqlite')

def make_chunk_features(chunk):
    """ Make all queries to build the chunk's features and return X and Y
    """
    #Y_train = Y_train.append(chunk['IsClick'])
    Y_train_temp = chunk['IsClick']
    del chunk['IsClick']
    X_train_temp = chunk[['HistCTR', 'Position', 'AdID']]
    adids = X_train_temp['AdID'].unique()
#    searchids = X_train_temp['SearchID'].unique()
    
    visit_temp = pd.read_sql_query("SELECT AdID, count(*) as c_visit FROM VisitsStream WHERE AdID in (" + ",".join(map(str, adids)) + ") GROUP BY AdID;", engine)
    phone_temp = pd.read_sql_query("SELECT AdID, count(*) as c_phone FROM PhoneRequestsStream WHERE AdID in (" + ",".join(map(str, adids)) + ") GROUP BY AdID;", engine)
    search_temp = pd.read_sql_query("SELECT AdID, count(*) as c_search FROM trainSearchRandom WHERE AdID in (" + ",".join(map(str, adids)) + ") GROUP BY AdID;", engine2)
    
    visit_temp['c_ratio_visit'] = visit_temp['c_visit'] / search_temp['c_search'].astype(float)
    phone_temp['c_ratio_phone'] = phone_temp['c_phone'] / search_temp['c_search'].astype(float)    
    
    # AdID, LocationID, CategoryID, Params, Price, Title
    #TODO: Params and Title are ignored
 #   ads_temp = pd.read_sql_query("SELECT AdID, LocationID as AdLocationID, CategoryID as AdCategoryID, Price FROM AdsInfo where AdID in (" + ",".join(map(str, adids)) + ");", engine)
    
    # SearchID, SearchDate, UserID, IsUserLoggedOn, IPID, SearchQuery, SearchLocationID, SearchCategoryID, SearchParams
    #TODO: SearchQuery and SearchParams are ignored 
    #search_temp = pd.read_sql_query("SELECT SearchID, SearchDate, UserID, IsUserLoggedOn, IPID, SearchLocationID, \
    #                            SearchCategoryID FROM SearchInfo where SearchID in " + str(tuple(searchids)), engine, parse_dates=['SearchDate'])
#    search_temp = pd.read_sql_query("SELECT SearchID, SearchDate, UserID, IsUserLoggedOn, IPID, LocationID as SearchLocationID, CategoryID as SearchCategoryID FROM SearchInfo where SearchID in (" + ",".join(map(str, searchids)) + ");", engine, parse_dates=['SearchDate'])

    # UserID, UserAgentID, UserAgentFamilyID, UserAgentOSID, UserDeviceID
#    user_ids = search_temp['UserID'].unique()
#    user_temp = pd.read_sql_query("SELECT UserID, UserAgentID, UserAgentFamilyID, UserDeviceID FROM UserInfo where UserID in (" + ",".join(map(str, user_ids)) + ");", engine)
    
    # LocationID, Level, RegionID, CityID
#    sloc_ids = search_temp['SearchLocationID'].unique()
#    sloc_temp = pd.read_sql_query("SELECT LocationID as SearchLocationID, Level as SearchLocLevel, RegionID as SearchRegionID, CityID as SearchCityID FROM Location where SearchLocationID in (" + ",".join(map(str, sloc_ids)) + ");", engine)
    
    # CategoryID, Level, ParentCategoryID, SubcategoryID
#    scat_ids = search_temp['SearchCategoryID'].unique()
#    scat_temp = pd.read_sql_query("SELECT CategoryID as SearchCategoryID, Level as SearchCatLevel, ParentCategoryID as SearchParentCategoryID, SubcategoryID as SearchSubcategoryID FROM Category where SearchCategoryID in (" + ",".join(map(str, scat_ids)) + ");", engine)

    # Ad info
    # LocationID, Level, RegionID, CityID
#    aloc_ids = ads_temp['AdLocationID'].unique()
#    aloc_ids = [a for a in aloc_ids if a]
#    aloc_temp = pd.read_sql_query("SELECT LocationID as AdLocationID, Level as AdLocLevel, RegionID as AdRegionID, CityID as AdCityID FROM Location where AdLocationID in (" + ",".join(map(str, aloc_ids)) + ");", engine)
    
    # CategoryID, Level, ParentCategoryID, SubcategoryID
#    acat_ids = ads_temp['AdCategoryID'].unique()
#    acat_ids = [a for a in acat_ids if a]
#    acat_temp = pd.read_sql_query("SELECT CategoryID as AdCategoryID, Level as AdCatLevel, ParentCategoryID as AdParentCategoryID, SubcategoryID as AdSubcategoryID FROM Category where AdCategoryID in (" + ",".join(map(str, acat_ids)) + ");", engine)
    
    # Join tables
    X_train_temp = pd.merge(X_train_temp, visit_temp, how='left', on=['AdID'])
    X_train_temp = pd.merge(X_train_temp, phone_temp, how='left', on=['AdID'])
    X_train_temp = pd.merge(X_train_temp, search_temp, how='left', on=['AdID'])
#    X_train_temp = pd.merge(X_train_temp, ads_temp, how='left', on=['AdID'])
#    X_train_temp = pd.merge(X_train_temp, search_temp, how='left', on=['SearchID'])    
#    X_train_temp = pd.merge(X_train_temp, user_temp, how='left', on=['UserID']) 
    
#    X_train_temp = pd.merge(X_train_temp, sloc_temp, how='left', on=['SearchLocationID'])
#    X_train_temp = pd.merge(X_train_temp, scat_temp, how='left', on=['SearchCategoryID'])
#    X_train_temp = pd.merge(X_train_temp, aloc_temp, how='left', on=['AdLocationID'])
#    X_train_temp = pd.merge(X_train_temp, acat_temp, how='left', on=['AdCategoryID'])

    # Add visit and phone request stats
#    user_ad = zip(X_train_temp['UserID'], X_train_temp['AdID'])
#    counts_visit, counts_phone = np.zeros(len(user_ad)), np.zeros(len(user_ad))
#    for i, u_a in enumerate(user_ad):
#        counts_visit[i] = pd.read_sql_query("SELECT count(*) as c FROM VisitsStream WHERE UserID=" + str(u_a[0]) + " AND AdID=" + str(u_a[1]), engine)['c'][0]
#        counts_phone[i] = pd.read_sql_query("SELECT count(*) as c FROM PhoneRequestsStream WHERE UserID=" + str(u_a[0]) + " AND AdID=" + str(u_a[1]), engine)['c'][0]
#
#    X_train_temp['visits'] = counts_visit
#    X_train_temp['phone_requests'] = counts_phone
#    del X_train_temp['SearchDate'] #TODO
#    X_train_temp = X_train_temp.replace('', 0, regex=True) # Replace empty strings by zeros
    print X_train_temp[:10]
    return X_train_temp.fillna(0), Y_train_temp

n_train, n_train_pos, losses = 0, 0, []
all_classes = np.array([0, 1])
#clf = PassiveAggressiveClassifier()
#TODO: 
clf = SGDClassifier(loss='log', n_jobs=-1) 
#clf = Perceptron() 
#clf = MultinomialNB(alpha=0.01)
t0 = time.time()    
tf = t0
#for irun, chunk in enumerate(pd.read_sql_query("SELECT * FROM trainSearchStream WHERE IsClick IN (0,1) ORDER BY RANDOM();", engine, chunksize=2000000)):
#for irun, chunk in enumerate(pd.read_sql_query("SELECT * FROM trainSearchStream WHERE IsClick IN (0,1);", engine, chunksize=2000000)):
for irun, chunk in enumerate(pd.read_sql_query("SELECT * FROM trainSearchRandom;", engine2, chunksize=2000000)):
    # for chunk in pd.read_sql_query("SELECT * FROM trainSearchStream", engine, chunksize=10000):
    ti = time.time()
    print "Query time: ", ti - tf
    if irun == 0:
        X_val, Y_val = make_chunk_features(chunk)
        tj = time.time()
        print "Make feature time: ", tj - ti
    else:
        X_train_temp, Y_train_temp = make_chunk_features(chunk)   
        tj = time.time()
        print "Make feature time: ", tj - ti
        clf.partial_fit(X_train_temp, Y_train_temp, classes=all_classes)
            
        n_train += len(X_train_temp)
        n_train_pos += sum(Y_train_temp)
        y_pred = clf.predict_proba(X_val.values.astype(float))
        logloss = log_loss(Y_val.values.astype(float), y_pred)
        losses.append(logloss)
        print "Logloss: ", logloss, "n_train: ", n_train, "n_train_pos: ", n_train_pos
        #s = clf.score(X_val.values.astype(float), Y_val.values.astype(float))
        #scores.append(s)
        #print "Score: ", s, "n_train: ", n_train, "n_train_pos: ", n_train_pos
    tf = time.time()
    print "Training time: ", tj - ti
y_pred = clf.predict_proba(X_val.values.astype(float))
logloss = log_loss(Y_val.values.astype(float), y_pred)
#print scores    
print "Logloss: ", logloss

#X_train.to_csv("X_train.csv", index=False)    
#Y_train.to_csv("Y_train.csv", index=False)
    
