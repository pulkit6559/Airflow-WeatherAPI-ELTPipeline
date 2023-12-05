from logging import exception
from typing import List, Optional, Tuple
import pandas as pd
from pandas.io.parsers import read_fwf
from weather_pipeline.transform import sql_queries

class Transform:
    def __init__(self, db_handler):
        self.db_handler = db_handler
        self.queries = []

    def run(self, query_name, **kwargs):
        # Check if the query_name is valid
        # if query_name not in self.queries:
        #     raise ValueError(f"Invalid query name: {query_name}")

        query_str = getattr(sql_queries, query_name)
        result = self.db_handler.execute_query(query_str.format(**kwargs))
        
        return result

