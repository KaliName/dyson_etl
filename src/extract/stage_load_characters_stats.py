import pandas as pd
import pymongo
import sqlalchemy

database_username = 'root'
database_password = 'pass'
database_ip= 'dbMysql:3306'
database_name = 'dyson_etl'



# Loading characters
charcters_stats = pd.read_csv("/usr/local/airflow/data/charcters_stats.csv")
print(charcters_stats.head(10))

# Creating connection
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password,
                                                      database_ip, database_name))

# Writting to DB
charcters_stats.to_sql(con=database_connection, name='stage_characters_stats', if_exists='replace')

print('Success Loading stage_load_characters_stats')