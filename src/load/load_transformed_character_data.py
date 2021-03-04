import pandas as pd
import pymongo
import sqlalchemy


def to_json_attributes(df, columns, new_col_name):
    df[new_col_name] = df[columns].apply(lambda x: x.to_json(), axis=1)
    new_df = df.drop(columns, axis=1)
    return new_df

# Setup DB Envoiroment
database_username = 'root'
database_password = 'pass'
database_ip= 'dbMysql:3306'
database_name = 'dyson_etl'

database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password,
                                                      database_ip, database_name))



# Read data
characters_df = pd.read_sql('SELECT * FROM transformed_final_character_stats_and_attrib', con=database_connection)
# print(characters_df.head(5))


myclient = pymongo.MongoClient("mongodb://mongo:27017/")
mydb = myclient["dyson_etl"]
mycol = mydb["prod_final_character_stats_and_attrib"]

print(mycol)

x = mycol.insert_many(characters_df.to_dict('records'))




