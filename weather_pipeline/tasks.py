import datetime
import pandas as pd
import gzip
import shutil


from weather_pipeline.extract import Extractor
from weather_pipeline.load import Loader
from weather_pipeline.db_handler import DbHandler

from weather_pipeline.utils import _get_logger, station_url, station_data_url
from weather_pipeline.config import LOADER_DESTINATION, LOADER_CONNECTION_STRING

logger = _get_logger(name=__name__)

db_handler = DbHandler()


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
                ("ELEVATION", "REAL"), ("STATE", "VARCHAR"), ("NAME", "VARCHAR"), 
                ("GSN FLAG", "VARCHAR"), ("HCN/CRN FLAG", "VARCHAR"), 
                ("WMO ID", "VARCHAR")]
    pd_col_names = [col[0] for col in col_name_types]
    col_required = ["ID", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME"]
    req_col_name_type = [col for col in col_name_types if col[0] in col_required]
    
    stations_data = pd.read_fwf("tmp/cities.csv", header=None, colspecs=[(0, 11), (13,20),
                                                                         (22, 30), (32, 37), (39, 40),
                                                                         (42, 71), (73, 75), (77,79), (81,85)])
    stations_data.columns = pd_col_names
    
    stations_data['LATITUDE'] = stations_data['LATITUDE'].astype(float)
    stations_data['LONGITUDE'] = stations_data['LONGITUDE'].astype(float)
    stations_data['ELEVATION'] = stations_data['ELEVATION'].astype(float)
    
    stations_data = stations_data[col_required]
    
    stations_data = stations_data.loc[stations_data["ID"].str.startswith('GM', na=False)]
    print(stations_data.head())
    
    # define loader
    loader = Loader(db_handler) 
    loader.load_csv_to_db(stations_data, table_name = "stations", columns=req_col_name_type)
        
        
def ingest_extract_station_data() -> None:
 
    logger.info("Extracting and loading cities..")
    
    # get all station_id's
    result = db_handler.execute_query(f"""SELECT "ID" FROM stations;""")
    station_ids = [row[0] for row in result]
    
    print("stations_ids: ", station_ids)
    
    for station_id in station_ids:
        # define extractor
        extractor = Extractor(source_type="web", source_url=station_data_url.format(station_id=station_id), 
                              temp_location=f"tmp/{station_id}.csv.gz")
        web_extractor = extractor.init()
        web_extractor.get(chunk_size=10000)
        
        station_data_pth = f"tmp/{station_id}.csv.gz"
        
        try:
            with gzip.open(station_data_pth, 'rb') as f_in:
                with open(f'tmp/{station_id}.csv', 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        except:
            print(f"Unable to unzip : {station_id}")
            db_handler.execute_query(f"""DELETE FROM stations where "ID"='{station_id}';""")
             
    logger.info("Extraction of stations yearly data: Done") 


def ingest_load_station_data() -> None:
    logger.info("Extracting and loading cities..")
    
    # get all station_id's
    result = db_handler.execute_query(f"""SELECT "ID" FROM stations;""")
    station_ids = [row[0] for row in result]
    
    loader = Loader(db_handler)
    
    for station_id in station_ids[:5]:
        # define extractor
        station_csv_pth = f"tmp/{station_id}.csv"
        
        station_id_df = pd.read_csv(station_csv_pth, header=None)
        columns = ["ID", "DATE", "ELEMENT", "DATA_VALUE", "M-FLAG", "Q-FLAG", "S-FLAG", "OBS-TIME"]
        station_id_df.columns = columns
        
        required_col_types = [("ID", "VARCHAR"), ("DATE", "DATE"), ("ELEMENT", "VARCHAR"), ("DATA_VALUE", "REAL")]
        
        station_id_df = station_id_df[[t[0] for t in required_col_types]]
            
        loader.load_csv_to_db(station_id_df, table_name = f"{station_id}", columns=required_col_types)


def transform_create_dimention_tables() -> None:
    """
        PRCP = Precipitation (tenths of mm)
        SNOW = Snowfall (mm)
        SNWD = Snow depth (mm)
        TMAX = Maximum temperature (tenths of degrees C)
        TMIN = Minimum temperature (tenths of degrees C)
    """
    dimention_tables = ['PRCP', 'SNOW', 'SNWD', 'TMAX', 'TMIN']
    
    result = db_handler.execute_query(f"""SELECT "ID" FROM stations;""")
    station_ids = [row[0] for row in result]
    
    loader = Loader(db_handler)
    
    req_columns = [("station_ID", "VARCHAR"), ("DATE", "DATE"), ("value", "REAL")]
    
    for tb in dimention_tables:
        loader.create_table_if_not_exists(table=tb, columns=[("station_ID", "VARCHAR"), ("DATE", "DATE"), ("value", "REAL")])
        for station in station_ids[:5]:
            # get dimention values from station table
            element_data = db_handler.execute_query("""SELECT "DATE", "DATA_VALUE" 
                                                        FROM {station_id} 
                                                        WHERE {station_id}."ELEMENT" = '{element_id}'; 
                                                    """.format(station_id=station, element_id=tb))
            element_data = [[station]+[str(e) for e in row] for row in element_data]
            loader.load_list_to_db(element_data, table_name = f"{tb}", columns=req_columns)
        

    