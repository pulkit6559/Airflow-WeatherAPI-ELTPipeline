from logging import exception
from typing import List, Optional, Tuple
import pandas as pd
from pandas.io.parsers import read_fwf

class Transformer:
    def __init__(self, db_handler):
        self.db_handler = db_handler