from weather_pipeline.tasks import *
from weather_pipeline.utils import _get_logger

logger = _get_logger("Pipeline")

if __name__ == "__main__":
    logger.info("Executing pipeline to create a table as given in the "\
                "challenge example")
    
    # extracting and loading cities
    # ingest_extract_cities()
    
    # ingest_load_stations()
    # ingest_extract_station_data()
    ingest_load_station_data()
    city = "Berlin"
    logger.info("Pipeline execution complete check the database "\
                f"for {city}_rank table to run a query for percentile rank.")