import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('sqlite:////home/ubuntu/data/avito/db/database.sqlite')

tsv_file = '/home/ubuntu/data/avito/db/data/SearchInfo.tsv'
d = pd.read_csv(tsv_file, header=0, delimiter='\t', usecols=['SearchID'])
d['SearchID2'] = d['SearchID']
del d['SearchID']
d.to_sql(con=engine, name='new_SearchInfo', if_exists='append', flavor='sqlite')

d.to_sql(name='SearchInfoID', con=engine, flavor='sqlite', if_exists='replace', index=False, chunksize=10000)


UPDATE new_SearchInfo
SET SearchID2 = (SELECT SearchID2
                       FROM SearchInfoID);
                      
UPDATE new_SearchInfo SET NewSearchID = (SELECT SearchID FROM SearchInfo WHERE ROWID = SearchInfo.ROWID);

UPDATE new_SearchInfo SET NewSearchID = (SELECT SearchID2 FROM SearchInfoID WHERE new_SearchInfo.SearchID = SearchInfoID.SearchID2);

UPDATE new_SearchInfo SET NewSearchID = SearchID;

# Final Fix
ALTER TABLE SearchInfo ADD COLUMN SearchID2;
UPDATE SearchInfo SET SearchID2 = SearchID;
CREATE INDEX idx_searchID2 ON SearchInfo(SearchID2);


export TMPDIR=/home/ubuntu/data/tmp/sqlite/
