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
characters_stats_df = pd.read_sql('SELECT * FROM stage_characters_stats', con=database_connection)

superheroes_power_matrix_df = pd.read_sql('SELECT * FROM stage_superheroes_power_matrix', con=database_connection)

print('characters_stats_df')
print(len(characters_stats_df.index))
print(characters_stats_df.head(1))

print('superheroes_power_matrix_df')
print(len(superheroes_power_matrix_df.index))
print(superheroes_power_matrix_df.head(1))

# Character-Stats Transformation
columnd_name_stats = 'Stats'
stats_columns = list(characters_stats_df)
stats_columns.pop(0)
stats_columns.pop(0)
characters_stats_transformed = to_json_attributes(characters_stats_df, stats_columns, columnd_name_stats)
# print(characters_stats_transformed.loc[[1]])

# Characters_super_powers Transformation
powers_columnd_name = 'Powers'
power_columns = list(superheroes_power_matrix_df)
power_columns.pop(0)
power_columns.pop(0)
characters_super_powers_transformed = to_json_attributes(superheroes_power_matrix_df, power_columns, powers_columnd_name)


final_df = characters_stats_transformed.merge(characters_super_powers_transformed, on='Name', how='outer')

final_df = final_df.drop('index_x', 1)

# Writting to DB
final_df.to_sql(con=database_connection, name='transformed_character_stats_and_attrib', if_exists='replace')

print('Success Loading marvel_characters_info')
