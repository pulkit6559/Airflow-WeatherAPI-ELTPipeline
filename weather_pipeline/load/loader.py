from logging import exception
from typing import List, Optional, Tuple
import pandas as pd
from pandas.io.parsers import read_fwf

# import psycopg2
# from psycopg2 import sql

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

class Loader:
    def __init__(self, db_handler):
        self.db_handler = db_handler
    
    def create_table_if_not_exists(self, table, columns):
        # Create a formatted string for column definitions
        columns_str = ', '.join([f'{name} {type}' for name, type in columns])

        query = """CREATE TABLE IF NOT EXISTS {table} 
                    ({columns_str})""".format(table=table, columns_str=columns_str)

        self.db_handler.execute_query(query)

    def load_csv_to_db(self, data, table_name, columns):

        self.create_table_if_not_exists(table_name, columns)

        # Extract column names for the INSERT query
        columns_str = ", ".join([f'"{name}"' for name, _ in columns])
        
        # data.to_sql(f'{table_name}', self.db_handler.engine, if_exists='replace', index=False)

        # Create a formatted string for the INSERT query
        values_str = '%s, %s, %s, %s, %s'
        insert_query = """INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})""".format(
            table_name=table_name,
            columns_str=columns_str,
            values_str=values_str
        )

        print(tuple(data.iloc[i,:].astype(str).values for i in range(4)))

        # Execute the INSERT query
        self.db_handler.execute_query(insert_query, tuple(data.iloc[i,:].astype(str).values for i in range(4)))

        result = self.db_handler.execute_query(f"SELECT * FROM {table_name} LIMIT 10")
        print([row for row in result])


# class Loader:
#     def __init__(self):
#         self.db = DbHandler()
    
#     def table_exists(self, table_name):
#         connection = self.db.get_connection()
#         cursor = connection.cursor()
        
#         try:
#             cursor.execute(
#                 "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = %s)",
#                 (table_name,)
#             )
#             return cursor.fetchone()[0]

#         finally:
#             cursor.close()
#             connection.close()
    
#     def create_table_if_not_exists(self, table, columns):
#         connection = self.db.get_connection()
#         cursor = connection.cursor()

#         try:
#             # Create a formatted string for column definitions
#             columns_str = ', '.join([f'{name} {type}' for name, type in columns])

#             create_table_query = f"""
#                 CREATE TABLE IF NOT EXISTS {table} (
#                     {columns_str}
#                 )
#             """
#             cursor.execute(create_table_query)
#             connection.commit()

#         finally:
#             cursor.close()
#             connection.close()
    
#     def load_csv_to_db(self, data, table_name, columns):
        
#         connection = self.db.get_connection()
#         cursor = connection.cursor()
        
#         if not self.table_exists(table_name):
#             self.create_table_if_not_exists(table_name, columns)
#         else:
#             try:
#                 columns_str = ', '.join([f'{name}' for name, type in columns])
#                 insert_query = f"""INSERT INTO your_table {columns_str} VALUES (%s, %s)
                
#                 """
#                 cursor.execute(insert_query, (data['column1'], data['column2']))
#                 connection.commit()

#             finally:
#                 cursor.close()
#                 connection.close()
            

