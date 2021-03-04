import pandas as pd
import pymongo
import sqlalchemy

database_username = 'root'
database_password = 'pass'
database_ip= 'dbMysql:3306'
database_name = 'dyson_etl'



# Loading characters
characters_to_comics = pd.read_csv("/usr/local/airflow/data/charactersToComics.csv")
print(characters_to_comics.head(10))

# Creating connection
database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password,
                                                      database_ip, database_name))

# Writting to DB
characters_to_comics.to_sql(con=database_connection, name='stage_characters_to_comics', if_exists='replace')

print('Success Loading marvel_characters_info')