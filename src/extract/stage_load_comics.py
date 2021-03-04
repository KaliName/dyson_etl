import pandas as pd
import pymongo
import sqlalchemy

# DB connstants
database_username = 'root'
database_password = 'pass'
database_ip= 'dbMysql:3306'
database_name = 'dyson_etl'

# Loading characters
characters = pd.read_csv("/usr/local/airflow/data/characters.csv")

print(characters.head(10))

# Creating connection
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password,
                                                      database_ip, database_name))

# Writting to DB
characters.to_sql(con=database_connection, name='stage_characters', if_exists='replace')

print('Success Loading Characters')
