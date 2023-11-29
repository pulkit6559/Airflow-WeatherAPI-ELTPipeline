import datetime
import pandas as pd

from weather_pipeline.extract import Extractor
from weather_pipeline.load import Loader
from weather_pipeline.db_handler import DbHandler

from weather_pipeline.utils import _get_logger, station_url
from weather_pipeline.config import LOADER_DESTINATION, LOADER_CONNECTION_STRING

logger = _get_logger(name=__name__)


def ingest_extract_stations() -> None:
    """ ingest_extract_cities
        This function is responsible to fetch the data for
        all german cities and load it to the final db.
    """
    
    logger.info("Extracting and loading cities..")
    url = station_url
    
    # define extractor
    extractor = Extractor(source_type="web", source_url=url, temp_location="tmp/cities.csv")
    
    # init extractor
    web_extractor = extractor.init()
    web_extractor.get(chunk_size=10000)
    
    logger.info("Extraction of stations: Done")
    
    
def ingest_load_stations() -> None:
    
    col_name_types = [("ID", "VARCHAR"), ("LATITUDE", "REAL"), ("LONGITUDE", "REAL"), 
                ("ELEVATION", "REAL"), ("NAME", "VARCHAR"), 
                ("GSN FLAG", "VARCHAR"), ("HCN/CRN FLAG", "VARCHAR"), 
                ("WMO ID", "VARCHAR")]
    pd_col_names = [col[0] for col in col_name_types]
    col_required = ["ID", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME"]
    req_col_name_type = [col for col in col_name_types if col[0] in col_required]
    
    stations_data = pd.read_fwf("tmp/cities.csv", header=None)
    print(stations_data.head())
    stations_data.columns = pd_col_names
    
    stations_data['LATITUDE'] = stations_data['LATITUDE'].astype(float)
    stations_data['LONGITUDE'] = stations_data['LONGITUDE'].astype(float)
    stations_data['ELEVATION'] = stations_data['ELEVATION'].astype(float)
    
    stations_data = stations_data[col_required]
    
    db_handler = DbHandler()
    
    # define loader
    loader = Loader(db_handler) 
    loader.load_csv_to_db(stations_data, table_name = "stations", columns=req_col_name_type)
        
        
    
    # # init loader
    # db_loader = loader.init()

    # # loading data as chunk
    # db_loader.load_csv_data(filters=[("typ", "Stadt")],
    #                         col_required = ["name", "lat", "lon"],
    #                         error_bad_lines=False, 
    #                         encoding="utf-8", 
    #                         delimiter="\t")
    