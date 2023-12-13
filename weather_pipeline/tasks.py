import datetime
import pandas as pd
import gzip
import shutil
import json
import os


from weather_pipeline.extract import Extractor
from weather_pipeline.load import Loader
from weather_pipeline.transform import Transform
from weather_pipeline.db_handler import DbHandler

from weather_pipeline.utils import _get_logger, station_url, station_data_url, cities_csv_url
from weather_pipeline.config import LOADER_DESTINATION, LOADER_CONNECTION_STRING, S3_BUCKET, S3_DIR

from aws_tasks import s3_single_upload, s3_multipart_upload

logger = _get_logger(name=__name__)

db_handler = DbHandler(host='default')


def ingest_extract_cities() -> None:
    """
    Extracts city data from a web source and saves it to a temporary CSV file.
    """
    
    logger.info("Extracting and loading cities..")
    url = cities_csv_url
    
    # define extractor
    extractor = Extractor(source_type="web", source_url=url, temp_location="tmp/cities.csv")
    
    # init extractor
    web_extractor = extractor.init()
    web_extractor.get(chunk_size=10000)
    
    logger.info("Extraction of cities: Done")


def ingest_load_cities() -> None:
    """
    Loads city data from the temporary CSV file to the 'cities' table in the database.
    """
    
    col_name_types = [("CITY", "VARCHAR PRIMARY KEY"), ("LATITUDE", "REAL"), ("LONGITUDE", "REAL")]
    
    cities_data = pd.read_csv("tmp/cities.csv")
    
    cities_data['lat'] = cities_data['lat'].astype(float)
    cities_data['lng'] = cities_data['lng'].astype(float)
    
    cities_data = cities_data[['city', 'lat', 'lng']]
    cities_data.columns = [col[0] for col in col_name_types]
    
    # define loader
    loader = Loader(db_handler) 
    loader.load_csv_to_db(cities_data, table_name = "cities", columns=col_name_types)


def ingest_extract_stations() -> None:
    """
    Extracts station data from a web source and saves it to a temporary CSV file.
    """
    
    logger.info("Extracting and loading cities..")
    url = station_url
    
    # define extractor
    extractor = Extractor(source_type="web", source_url=url, temp_location="tmp/stations.csv")
    
    # init extractor
    web_extractor = extractor.init()
    web_extractor.get(chunk_size=10000)
    
    logger.info("Extraction of stations: Done")
    
    
def ingest_load_stations() -> None:
    """
    Loads station data from the temporary CSV file to the 'stations' table in the database.
    """
    
    col_name_types = [("ID", "VARCHAR"), ("LATITUDE", "REAL"), ("LONGITUDE", "REAL"), 
                ("ELEVATION", "REAL"), ("STATE", "VARCHAR"), ("NAME", "VARCHAR"), 
                ("GSN FLAG", "VARCHAR"), ("HCN/CRN FLAG", "VARCHAR"), 
                ("WMO ID", "VARCHAR")]
    pd_col_names = [col[0] for col in col_name_types]
    col_required = ["ID", "LATITUDE", "LONGITUDE", "ELEVATION", "NAME"]
    req_col_name_type = [col for col in col_name_types if col[0] in col_required]
    
    stations_data = pd.read_fwf("tmp/stations.csv", header=None, colspecs=[(0, 11), (13,20),
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


def transform_stations():
    """
    Transforms the 'stations' table in the database by adding a 'city' column and updating it
    based on the closest city using latitude and longitude.
    """
    
    db_handler.execute_query(f"""ALTER TABLE stations
                                    ADD city VARCHAR; """)
    
    db_handler.execute_query(f"""UPDATE stations
                                    SET city = closest.city
                                    FROM (
                                        SELECT
                                            stations.id,
                                            cities.city,
                                            ROW_NUMBER() OVER (PARTITION BY stations.id ORDER BY point(stations.latitude, stations.longitude) <@> point(cities.latitude, cities.longitude)) AS rn
                                        FROM
                                            cities
                                            CROSS JOIN stations
                                    ) AS closest
                                    WHERE stations.id = closest.id AND closest.rn = 1;
                             """)
    # rows = db_handler.execute_query(f"SELECT * FROM stations LIMIT 100;")
    # print([row for row in rows])
        
        
def ingest_extract_station_data() -> None:
    """
    Extracts yearly data for stations from a web source and saves it to temporary CSV files.
    """
 
    logger.info("Extracting and loading cities..")
    
    # get all station_id's
    result = db_handler.execute_query(f"""SELECT id FROM stations;""")
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


def ingest_load_station_data(mode="PG_CONN") -> None:
    """
    Loads yearly station data from temporary CSV files to corresponding tables in the database.
    Args:
        mode (str): Mode for data loading, options are 'PG_CONN' for PostgreSQL connection or 'DMS' for AWS DMS.
    """
    
    logger.info("Extracting and loading cities..")
    
    # get all station_id's
    result = db_handler.execute_query(f"""SELECT ID FROM stations;""")
    station_ids = [row[0] for row in result]
    
    loader = Loader(db_handler)
    
    for station_id in station_ids[:5]:
        # define extractor
        if mode=="DMS":
            print("DMS Ingestion: TBD")
            # multipart upload -> s3_upload
            # s3_multipart_upload(file_path=f"tmp/{station_id}.csv", 
            #                     bucket_name="iambucketnew", 
            #                     object_key=f"stations_data/{station_id}.csv",
            #                     region_name='eu-central-1')
        
            # create table {station_id}
            
            # dms setup -> create_dms_task
            # dms migrate -> migrate file
            
            continue
        else:
            station_csv_pth = f"tmp/{station_id}.csv"
            
            station_id_df = pd.read_csv(station_csv_pth, header=None)
            columns = ["ID", "DATE", "ELEMENT", "DATA_VALUE", "M-FLAG", "Q-FLAG", "S-FLAG", "OBS-TIME"]
            station_id_df.columns = columns
            
            required_col_types = [("ID", "VARCHAR"), ("DATE", "DATE"), ("ELEMENT", "VARCHAR"), ("DATA_VALUE", "REAL")]
            
            station_id_df = station_id_df[[t[0] for t in required_col_types]]
                
            loader.load_csv_to_db(station_id_df, table_name = f"{station_id}", columns=required_col_types)


def transform_create_dimention_tables() -> None:
    """
    Creates dimension tables (e.g., TMAX, TMIN) in the database for storing specific weather elements.

        PRCP = Precipitation (tenths of mm)
        SNOW = Snowfall (mm)
        SNWD = Snow depth (mm)
        TMAX = Maximum temperature (tenths of degrees C)
        TMIN = Minimum temperature (tenths of degrees C)
    """
    dimention_tables = ['TMAX', 'TMIN']
    
    result = db_handler.execute_query(f"""SELECT ID FROM stations;""")
    station_ids = [row[0] for row in result]
    
    loader = Loader(db_handler)
    
    req_columns = [("station_ID", "VARCHAR"), ("DATE", "DATE"), ("value", "REAL")]
    
    for tb in dimention_tables:
        loader.create_table_if_not_exists(table=tb, columns=[("station_ID", "VARCHAR"), ("DATE", "DATE"), ("value", "REAL")])
        for station in station_ids[:5]:
            # get dimention values from station table
            element_data = db_handler.execute_query("""SELECT DATE, DATA_VALUE
                                                        FROM {station_id} 
                                                        WHERE {station_id}.ELEMENT = '{element_id}'; 
                                                    """.format(station_id=station, element_id=tb))
            element_data = [[station]+[str(e) for e in row] for row in element_data]
            loader.load_list_to_db(element_data, table_name = f"{tb}", columns=req_columns)
        

def transform_station_monthly_temp_avg() -> None:
    """
    Computes the monthly average temperature for each station and uploads the result to both local and S3 storage.
    """
    
    transform = Transform(db_handler)
    result_cursor = transform.run("monthly_avg_by_station", table="TMAX", start_year="1990", end_year="2000")
    
    result = [row for row in result_cursor]
    print(result)
    
    new_dict = {}
    
    for row in result:
        station_id, month, avg_temperature = row
        # print(type(month))
        # Initialize station entry if not exists
        new_dict.setdefault(station_id, {"max_avg": {}})

        new_dict[station_id]["max_avg"][str(month)] = avg_temperature

    with open('tmp/stations_avg_temp.json', 'w') as fp:
        json.dump(new_dict, fp)
    
    s3_single_upload('tmp/stations_avg_temp.json', S3_BUCKET, S3_DIR+"/stations_avg_temp.json")
    
    print(new_dict)
    
def transform_get_avg_top5():
    """ 
    Retrieves the average TMAX values for the 5 closest stations for each city from the 'cities' table
    Execute a SQL query on a PostgreSQL database, save the result to a CSV file, and upload it to Amazon S3.
    """
    
    rows = db_handler.execute_query("""
                                WITH ranked_stations AS (
                                    SELECT
                                        c.city,
                                        s.id AS station_id,
                                        s.latitude AS station_lat,
                                        s.longitude AS station_long,
                                        t.value AS tmax,
                                        ROW_NUMBER() OVER (PARTITION BY c.city, t.date ORDER BY point(c.latitude, c.longitude) <-> point(s.latitude, s.longitude)) AS station_rank
                                    FROM
                                        cities c
                                        CROSS JOIN stations s
                                        JOIN tmax t ON s.id = t.station_ID
                                )
                                , top_5_stations AS (
                                    SELECT
                                        city,
                                        station_id,
                                        station_lat,
                                        station_long,
                                        tmax
                                    FROM
                                        ranked_stations
                                    WHERE
                                        station_rank <= 5
                                )
                                , city_stats AS (
                                    SELECT
                                        city,
                                        AVG(tmax) AS avg_tmax,
                                        PERCENT_RANK() OVER (ORDER BY AVG(tmax)) AS percentile_count
                                    FROM
                                        top_5_stations
                                    GROUP BY
                                        city
                                )
                                SELECT
                                    c.*,
                                    cs.avg_tmax,
                                    cs.percentile_count
                                FROM
                                    cities c
                                    LEFT JOIN city_stats cs ON c.city = cs.city;
                                """)
    rows_list = [row for row in rows]
    
    df = pd.DataFrame(rows_list, columns=[desc for desc in rows.keys()])
    df.to_csv('tmp/cities_avg_tmax_5_stations.csv', index=False)
    
    s3_single_upload('tmp/cities_avg_tmax_5_stations.csv', S3_BUCKET, S3_DIR+"/cities_avg_tmax_5_stations.csv")
    
    