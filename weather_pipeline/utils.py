import logging

station_url = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/ghcnd-stations.txt"
station_data_url = "https://www.ncei.noaa.gov/pub/data/ghcn/daily/by_station/{station_id}.csv.gz"
cities_csv_url = "https://simplemaps.com/static/data/country-cities/de/de.csv"

def _get_logger(name):
    """
    Single function to implemnt logging throughout the application,
    can be extended to add multiple handler to provide multiclient
    logging
    """
    
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
    logger = logging.getLogger(name)
    return logger