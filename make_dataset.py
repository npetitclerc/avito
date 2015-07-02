import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('sqlite:////home/ubuntu/data/avito/db/database.sqlite')

X_train = pd.DataFrame()
Y_train = pd.DataFrame()

#for chunk in pd.read_sql_query("SELECT * FROM trainSearchStream", engine, chunksize=1000):
for chunk in pd.read_sql_query("SELECT * FROM trainSearchStream limit 10", engine, chunksize=5):

    Y_train_temp = chunk['IsClick']
    del chunk['IsClick']
    X_train_temp = chunk
    adids = X_train_temp['AdID'].unique()
    searchids = X_train_temp['SearchID'].unique()
    
    # AdID, LocationID, CategoryID, Params, Price, Title
    #TODO: Params and Title are ignored
    ads_temp = pd.read_sql_query("SELECT AdID, LocationID, CategoryID, Price FROM AdsInfo where AdID in " + str(tuple(adids)), engine)
    
    # SearchID, SearchDate, UserID, IsUserLoggedOn, IPID, SearchQuery, SearchLocationID, SearchCategoryID, SearchParams
    #TODO: SearchQuery and SearchParams are ignored 
    #search_temp = pd.read_sql_query("SELECT SearchID, SearchDate, UserID, IsUserLoggedOn, IPID, SearchLocationID, \
    #                            SearchCategoryID FROM SearchInfo where SearchID in " + str(tuple(searchids)), engine, parse_dates=['SearchDate'])
    search_temp = pd.read_sql_query("SELECT SearchID, SearchDate, UserID, IsUserLoggedOn, IPID, LocationID as SearchLocationID, CategoryID as SearchCategoryID FROM SearchInfo where SearchID in (" + ",".join(map(str, searchids)) + ");", engine, parse_dates=['SearchDate'])

    # UserID, UserAgentID, UserAgentFamilyID, UserAgentOSID, UserDeviceID
    user_ids = search_temp['UserID'].unique()
    user_temp = pd.read_sql_query("SELECT UserID, UserAgentID, UserAgentFamilyID, UserDeviceID FROM UserInfo where UserID in " + str(tuple(user_ids)), engine)
    
    # LocationID, Level, RegionID, CityID
    sloc_ids = search_temp['SearchLocationID'].unique()
    sloc_temp = pd.read_sql_query("SELECT LocationID as SearchLocationID, Level as SearchLocLevel, RegionID as SearchRegionID, CityID as SearchCityID FROM Location where SearchLocationID in " + str(tuple(sloc_ids)), engine)
    
    # CategoryID, Level, ParentCategoryID, SubcategoryID
    scat_ids = search_temp['SearchCategoryID'].unique()
    scat_temp = pd.read_sql_query("SELECT CategoryID as SearchCategoryID, Level as SearchCatLevel, ParentCategoryID as SearchParentCategoryID, SubcategoryID as SearchSubcategoryID FROM Category where SearchCategoryID in " + str(tuple(scat_ids)), engine)
    

    #TODO Join tables by AdID and SearchID
    X_train_temp = pd.merge(X_train_temp, ads_temp, how='left', on=['AdID'])
    X_train_temp = pd.merge(X_train_temp, search_temp, how='left', on=['SearchID'])    
    X_train_temp = pd.merge(X_train_temp, user_temp, how='left', on=['UserID']) 
    
    X_train_temp = pd.merge(X_train_temp, sloc_temp, how='left', on=['SearchLocationID'])
    X_train_temp = pd.merge(X_train_temp, scat_temp, how='left', on=['SearchCategoryID'])
    
    
    
    