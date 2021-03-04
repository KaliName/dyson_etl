import pandas as pd
import pymongo
import sqlalchemy

# Setup DB Envoiroment
database_username = 'root'
database_password = 'pass'
database_ip= 'dbMysql:3306'
database_name = 'dyson_etl'

database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password,
                                                      database_ip, database_name))



# Read data
stage_comics_df = pd.read_sql('SELECT * FROM stage_comics', con=database_connection)

characters_to_comics_df = pd.read_sql('SELECT * FROM  stage_characters_to_comics', con=database_connection)



final_df = stage_comics_df.merge(characters_to_comics_df, on='comicID', how='left')

final_df.to_sql(con=database_connection, name='transfomed_character_to_comic', if_exists='replace')

print('Success Loading marvel_characters_info')
