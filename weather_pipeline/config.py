import os

LOADER_DESTINATION = os.getenv("LOADER_DESTINATION", "rdbms")

# airflow
LOADER_CONNECTION_STRING = os.getenv("LOADER_CONNECTION_STRING", "postgresql://airflow:airflow@postgres:5432/postgres")

S3_BUCKET = "iambucketnew"

S3_DIR = "weatherdb/transforms"

# no airflow
# LOADER_CONNECTION_STRING = os.getenv("LOADER_CONNECTION_STRING", "postgresql://postgres:admin@localhost:5432/postgres")