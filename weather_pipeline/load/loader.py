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
    
    def table_exists(self, table_name):
        query = f"SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{table_name}')"
        result = self.db_handler.execute_query(query)
        return result.scalar()

    def create_table_if_not_exists(self, table, columns):
        # Create a formatted string for column definitions
        columns_str = ', '.join([f'{name} {type}' for name, type in columns])

        query = f"""
            CREATE TABLE IF NOT EXISTS {table} (
                {columns_str}
            )
        """
        self.db_handler.execute_query(query)

    def load_csv_to_db(self, data, table_name, columns):
        if not self.table_exists(table_name):
            self.create_table_if_not_exists(table_name, columns)

        # Extract column names for the INSERT query
        columns_str = ', '.join([f'{name}' for name, _ in columns])

        # Create a formatted string for the INSERT query
        values_str = ', '.join(['%s' for _ in columns])
        insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({values_str})"

        # Execute the INSERT query
        self.db_handler.execute_query(insert_query, tuple(data[col] for col, _ in columns))



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
            

