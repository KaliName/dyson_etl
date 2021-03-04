import pandas as pd
import pymongo
import sqlalchemy

database_username = 'root'
database_password = 'pass'
database_ip= 'dbMysql:3306'
database_name = 'dyson_etl'



# Loading characters
stage_comics = pd.read_csv("/usr/local/airflow/data/comics.csv")
print(stage_comics.head(10))

# Creating connection
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password,
                                                      database_ip, database_name))

# Writting to DB
stage_comics.to_sql(con=database_connection, name='stage_comics', if_exists='replace')

print('Success Loading stage_comics')
