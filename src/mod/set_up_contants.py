import sqlalchemy

class set_up_contants:
    def get_database_connection(self):

        dag = 'dbMysql:3306'
        local = 'local:3306'

        database_username = 'root'
        database_password = 'pass'
        database_ip= 'dbMysql:3306'
        database_name = 'dyson_etl'

        database_connection = sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password,
                                                      database_ip, database_name))

        # Creating connection
        return database_connection