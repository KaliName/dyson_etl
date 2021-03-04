import pandas as pd
import pymongo
import sqlalchemy

database_username = 'root'
database_password = 'pass'
database_ip= 'dbMysql:3306'
database_name = 'dyson_etl'



# Loading characters
marvel_dc_characters = pd.read_excel("/usr/local/airflow/data/marvel_dc_characters.csv")
print(marvel_dc_characters.head(10))

# Creating connection
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password,
                                                      database_ip, database_name))

# Writting to DB
marvel_dc_characters.to_sql(con=database_connection, name='stage_marvel_dc_characters', if_exists='replace')

print('Success Loading stage_comics')
