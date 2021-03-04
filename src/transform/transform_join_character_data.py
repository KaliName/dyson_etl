import pandas as pd
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
characters_stats_df = pd.read_sql('SELECT * FROM stage_characters', con=database_connection)

superheroes_power_matrix_df = pd.read_sql('SELECT * FROM transformed_character_stats_and_attrib', con=database_connection)


print('characters_stats_df')
print(len(characters_stats_df.index))
print(characters_stats_df.head(1))

print('superheroes_power_matrix_df')
print(len(superheroes_power_matrix_df.index))
print(superheroes_power_matrix_df.head(1))

# Merging characterI stats and id
characters_df_final = characters_stats_df.merge(superheroes_power_matrix_df, left_on='name', right_on='Name', how='inner')
characters_df_final = characters_df_final.drop('name', 1)
print(characters_df_final.head(1))

# Writting to DB
characters_df_final.to_sql(con=database_connection, name='transformed_final_character_stats_and_attrib', if_exists='replace')

print('Success Loading characters_df_final')
