import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('sqlite:////home/ubuntu/data/avito/db/database.sqlite')

tsv_file = '/Users/nicolas/Downloads/SearchInfo.tsv'
d = pd.read_csv(tsv_file, header=0, delimiter='\t', usecols=['SearchID'])

d.to_sql(con=engine, name='new_SearchInfo', if_exists='append', flavor='sqlite')


