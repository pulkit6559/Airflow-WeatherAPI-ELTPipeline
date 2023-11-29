# db_handler.py
from contextlib import contextmanager
# from config.config import DATABASE_CONFIG  # Import your database configuration from the config file
import configparser
  
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


class DbHandler:
    def __init__(self, config_file='tmp/config.ini'):
        config = configparser.ConfigParser()
        config.read(config_file)
        print(config.sections())
        db_user = config['database']['POSTGRES_USER']
        db_password = config['database']['POSTGRES_PASSWORD']
        db_host = config['database']['POSTGRES_HOST']
        db_port = config['database']['POSTGRES_PORT']
        db_name = config['database']['POSTGRES_DB']
        
        db_url = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'

        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)

    def create_session(self):
        return self.Session()

    def execute_query(self, query, values=None):
        with self.engine.connect() as connection:
            try:
                print(type(values), len(values))
            except:
                pass
            result = connection.execute(query, values)
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