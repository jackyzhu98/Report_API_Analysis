from queries import *
import mysql.connector

connection = mysql.connector.connect(
    host = db_host,
    database = api_db,
    username = db_user,
    password = password,
)
if connection.is_connected():
            print("MySQL connection is connected")
def basic_data_query(connection, raw_query_list):
    for i in raw_query_list:
        qur =  raw_query_dict[i]
        col =  raw_col_dict[i]
        df = query_func(connection = connection, query= qur, col_name= col)
        df.to_csv(os.path.join(output_path,i)+ '.csv')
connection.close()