from weather_pipeline.tasks import *
from weather_pipeline.utils import _get_logger

logger = _get_logger("Pipeline")

if __name__ == "__main__":
    logger.info("Executing pipeline to create a table as given in the "\
                "challenge example")
    
    # extracting and loading cities
    # ingest_extract_cities()
    # ingest_load_cities()
    # ingest_extract_stations()
    # ingest_load_stations()
    # transform_stations()
    # ingest_extract_station_data()
    # ingest_load_station_data()
    # transform_create_dimention_tables()
    # transform_station_monthly_temp_avg()
    transform_get_avg_top5()

    logger.info("Pipeline execution complete: " \
                f"Check transformations in s3://{S3_BUCKET}/{S3_DIR}")