import pymongo
import sqlalchemy
import pandas as pd

print("mongo.")

myclient = pymongo.MongoClient("mongodb://mongo:27017/")
mydb = myclient["mydatabase"]
mycol = mydb["customers5"]

mylist = [
    { "name": "John", "address": "Highway 37"},
    {"name": "Peter", "address": "Lowstreet 27"},
    {"name": "Amy", "address": "Apple st 652"}
]

x = mycol.insert_many(mylist)

# #print list of the _id values of the inserted documents:
# print(x.inserted_ids)

dblist = myclient.list_database_names()
if "mydatabase" in dblist:
    print("The database exists.")

print("<<---------------FINISH---------->>>")


print("<<---------------START DB---------->>>")
database_username = 'root'
database_password = 'pass'
database_ip= 'dbMysql:3306'
database_name = 'dyson_etl'

database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password,
                                                      database_ip, database_name))


new_df = pd.DataFrame(mylist, columns=['q_data'])

new_df.to_sql(con=database_connection, name='final_df3', if_exists='replace')


print("<<---------------FINISH---------->>>")