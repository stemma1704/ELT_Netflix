import pandas as pd
from sqlalchemy import create_engine
import pymysql


# Load data into a DataFrame
data = pd.read_csv('netflix_titles_2.csv')

# Create the MySQL connection engine
#engine = sal.create_engine('mysql+pymysql://root:mysql@127.0.0.1/etl')
engine = create_engine('mysql+pymysql://root:root123@localhost:3306/etl')

# Write DataFrame to MySQL
data.to_sql('netflix_raw', con=engine, index=True, if_exists='replace')



#conn=engine.connect()



# Write DataFrame to MySQL
(pd.read_csv('netflix_titles.csv')).to_sql('netflix_raw', con=engine, index=True, if_exists='replace')


# Display DataFrame
print((pd.read_csv('netflix_titles.csv')).head())


# # Filter DataFrame by show_id
# print((pd.read_csv('netflix_titles.csv'))[(pd.read_csv('netflix_titles.csv'))['show_id'] == 's5023'])


# # Find maximum length of descriptions
# print(max((pd.read_csv('netflix_titles.csv'))['description'].dropna().str.len()))


# # Find NaN values count
# print((pd.read_csv('netflix_titles.csv')).isna().sum())

