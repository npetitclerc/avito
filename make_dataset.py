import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('sqlite:////home/ubuntu/data/avito/db/database.sqlite')

X_train = pd.DataFrame()
Y_train = pd.DataFrame()

#for chunk in pd.read_sql_query("SELECT * FROM trainSearchStream limit 2000", engine, chunksize=1000):
for chunk in pd.read_sql_query("SELECT * FROM trainSearchStream", engine, chunksize=10000):

    Y_train = Y_train.append(chunk['IsClick'])
    del chunk['IsClick']
    X_train_temp = chunk
    adids = X_train_temp['AdID'].unique()
    searchids = X_train_temp['SearchID'].unique()
    
    # AdID, LocationID, CategoryID, Params, Price, Title
    #TODO: Params and Title are ignored
    ads_temp = pd.read_sql_query("SELECT AdID, LocationID as AdLocationID, CategoryID as AdCategoryID, Price FROM AdsInfo where AdID in (" + ",".join(map(str, adids)) + ");", engine)
    
    # SearchID, SearchDate, UserID, IsUserLoggedOn, IPID, SearchQuery, SearchLocationID, SearchCategoryID, SearchParams
    #TODO: SearchQuery and SearchParams are ignored 
    #search_temp = pd.read_sql_query("SELECT SearchID, SearchDate, UserID, IsUserLoggedOn, IPID, SearchLocationID, \
    #                            SearchCategoryID FROM SearchInfo where SearchID in " + str(tuple(searchids)), engine, parse_dates=['SearchDate'])
    search_temp = pd.read_sql_query("SELECT SearchID, SearchDate, UserID, IsUserLoggedOn, IPID, LocationID as SearchLocationID, CategoryID as SearchCategoryID FROM SearchInfo where SearchID in (" + ",".join(map(str, searchids)) + ");", engine, parse_dates=['SearchDate'])

    # UserID, UserAgentID, UserAgentFamilyID, UserAgentOSID, UserDeviceID
    user_ids = search_temp['UserID'].unique()
    user_temp = pd.read_sql_query("SELECT UserID, UserAgentID, UserAgentFamilyID, UserDeviceID FROM UserInfo where UserID in (" + ",".join(map(str, user_ids)) + ");", engine)
    
    # LocationID, Level, RegionID, CityID
    sloc_ids = search_temp['SearchLocationID'].unique()
    sloc_temp = pd.read_sql_query("SELECT LocationID as SearchLocationID, Level as SearchLocLevel, RegionID as SearchRegionID, CityID as SearchCityID FROM Location where SearchLocationID in (" + ",".join(map(str, sloc_ids)) + ");", engine)
    
    # CategoryID, Level, ParentCategoryID, SubcategoryID
    scat_ids = search_temp['SearchCategoryID'].unique()
    scat_temp = pd.read_sql_query("SELECT CategoryID as SearchCategoryID, Level as SearchCatLevel, ParentCategoryID as SearchParentCategoryID, SubcategoryID as SearchSubcategoryID FROM Category where SearchCategoryID in (" + ",".join(map(str, scat_ids)) + ");", engine)

    # Ad info
    # LocationID, Level, RegionID, CityID
    aloc_ids = ads_temp['AdLocationID'].unique()
    aloc_ids = [a for a in aloc_ids if a]
    aloc_temp = pd.read_sql_query("SELECT LocationID as AdLocationID, Level as AdLocLevel, RegionID as AdRegionID, CityID as AdCityID FROM Location where AdLocationID in (" + ",".join(map(str, aloc_ids)) + ");", engine)
    
    # CategoryID, Level, ParentCategoryID, SubcategoryID
    acat_ids = ads_temp['AdCategoryID'].unique()
    acat_ids = [a for a in acat_ids if a]
    acat_temp = pd.read_sql_query("SELECT CategoryID as AdCategoryID, Level as AdCatLevel, ParentCategoryID as AdParentCategoryID, SubcategoryID as AdSubcategoryID FROM Category where AdCategoryID in (" + ",".join(map(str, acat_ids)) + ");", engine)
    

    # Join tables
    X_train_temp = pd.merge(X_train_temp, ads_temp, how='left', on=['AdID'])
    X_train_temp = pd.merge(X_train_temp, search_temp, how='left', on=['SearchID'])    
    X_train_temp = pd.merge(X_train_temp, user_temp, how='left', on=['UserID']) 
    
    X_train_temp = pd.merge(X_train_temp, sloc_temp, how='left', on=['SearchLocationID'])
    X_train_temp = pd.merge(X_train_temp, scat_temp, how='left', on=['SearchCategoryID'])
    X_train_temp = pd.merge(X_train_temp, aloc_temp, how='left', on=['AdLocationID'])
    X_train_temp = pd.merge(X_train_temp, acat_temp, how='left', on=['AdCategoryID'])

    X_train = X_train.append(X_train_temp)
    
    print X_train_temp     
    
X_train.to_csv("X_train.csv", index=False)    
Y_train.to_csv("Y_train.csv", index=False)
    
