# db_handler.py
from contextlib import contextmanager
from config.config import DATABASE_CONFIG  # Import your database configuration from the config file
import configparser
  
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker



class DbHandler:
    def __init__(self, config_file='config.ini'):
        config = configparser.ConfigParser()
        config.read(config_file)

        db_user = config['database']['db_user']
        db_password = config['database']['db_password']
        db_host = config['database']['db_host']
        db_port = config['database']['db_port']
        db_name = config['database']['db_name']
        
        db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)

    def create_session(self):
        return self.Session()

    def execute_query(self, query):
        with self.engine.connect() as connection:
            result = connection.execute(query)
        return result


# class DbHandler:
#     def __init__(self):
#         self.conn = None

#     @contextmanager
#     def get_cursor(self):
#         try:
#             self.conn = psycopg2.connect(
#                 dbname=DATABASE_CONFIG['dbname'],
#                 user=DATABASE_CONFIG['user'],
#                 password=DATABASE_CONFIG['password'],
#                 host=DATABASE_CONFIG['host'],
#                 port=DATABASE_CONFIG['port']
#             )
#             with self.conn.cursor(cursor_factory=RealDictCursor) as cursor:
#                 yield cursor
#         finally:
#             if self.conn is not None:
#                 self.conn.close()

#     def execute_query(self, query, params=None):
#         with self.get_cursor() as cursor:
#             cursor.execute(query, params)
#             self.conn.commit()