# Airflow WeatherAPI ETL

This repository contains the code and files for an Airflow pipeline that performs ELT (Extract, Load, Transform) operations on yearly weather data from [ncei.noaa.gov](https://ncei.noaa.gov) for stations in Germany.

## Project Overview

The project consists of the following components:

- **weather_pipeline**: package that contains the modules and functions for ELT tasks that download csv's, process and write them to PostgreSQL, apply transformations.
The packages are separated into three parts:
    - Extract 
    - Load 
    - Transform 
- **pipeline.py**: A Python script that runs the pipeline as a standalone application without Airflow.
- **aws_tasks**: A folder that contains Python scripts for creating and deleting AWS resources such as creating S3 buckets and uploading files in single and multipart manner using the boto3 library.
- **dags**: contains the Airflow DAG 
- **tmp**: A folder that stores temporary files generated during the pipeline execution.
- **Dockerfile**: A file that defines the Docker image for running PostgreSQL containerized environment.

## Task separation
The ELT tasks are gouped by their broad conceptual functionality into three packages, each of which implements its own class which is modular to new functionalities 

1. **Extract**: 
    The `Extractor` class takes a resource url as input and downloads the data from websource, the class can be extended to add more sources depending on requirements

2. **Load**:
    The `Loader` class is used to write the data to `PostgreSQL`. It supports two methods for writing into the tables, through a csv or a list of rows  

3. **Transform**:
    Executes sql queries through a transformations handler class, the `transform_*` functions apply the respective transformations and save the result locally or on AWS S3
    ```
    transform = Transform(db_handler)
    result_cursor = transform.run("sql_query_name", **kwargs)
    ```

    - Transformations applied:
        -  
        - `transform_station_monthly_temp_avg()` \
            This function aims to derive the monthly average temperature for each weather station over a specific period (1990 to 2000). The underlying SQL query (monthly_avg_by_station) groups the entire 'TMAX' readings table based on the 'DATE' field. It employs the PERCENTILE_CONT window function within each group to calculate the median temperature for Germany over each day. The result is structured into a dictionary format containing station IDs, months, and corresponding average temperatures. \
            The derived information is stored locally as a JSON file (stations_avg_temp.json). The locally stored JSON file is uploaded to an Amazon S3 bucket for broader accessibility.
        - `transform_get_avg_top5()` \
            This function retrieves the average TMAX values for the five closest stations to each city from the 'cities' table. The SQL query involves the use of Common Table Expressions (CTEs) to calculate station rankings based on proximity. The result is saved locally as a CSV file and uploaded to Amazon S3.

    > _The current SQL query, although logically correct, can become resource-intensive when executed for an extensive number of table entries. To address this challenge and enhance resource efficiency, a potential optimization strategy involves the introduction of a precomputed table named `germany_means`.         
    This table acts as a repository for precomputed results, eliminating the need to recalculate medians for the same dates._



## Installation and Usage

To run the pipeline, you need to have the following prerequisites installed:

- [SQLAlchemy](^6^) 
- [Requests](^6^) 
- [Boto3](^6^) 
- [Airflow](^6^) 
- [Docker](^7^) 

```bash
pip install -r requirements.txt
```

You also need to have an AWS account and credentials, and set the following environment variables:

- `AWS_ACCESS_KEY_ID`: Your AWS access key ID
- `AWS_SECRET_ACCESS_KEY`: Your AWS secret access key
- `AWS_REGION`: Your AWS region

## Setup PostgreSQL

Follow these steps to set up a PostgreSQL container and enable the necessary extensions, specifically `earthdistance` and `cube`. These extensions are used for geospatial calculations for `latitute` and `longitute`.


### 1. Create a PostgreSQL Container

Create a new PostgreSQL container:

```bash
docker run --name postgres-container -e POSTGRES_USER=USERNAME -e POSTGRES_PASSWORD=your_password -e POSTGRES_DB=DBNAME -p 5432:5432 -d postgres:latest
```

Adjust the values for `-e POSTGRES_USER`, `-e POSTGRES_PASSWORD`, and `-e POSTGRES_DB` as needed.

### 3. Access the PostgreSQL Container

Access the PostgreSQL container's bash shell:

```bash
docker exec -it postgres-container bash
```

### 4. Execute SQL Commands

Inside the container's shell, execute the following SQL commands to create the required extensions:

```bash
psql -U USERNAME -d DBNAME -c "CREATE EXTENSION earthdistance;"
psql -U USERNAME -d DBNAME -c "CREATE EXTENSION cube;"
```

Replace `USERNAME` with your PostgreSQL username and adjust the database name (`DBNAME`).

### 5. Verify Extensions

To verify that the extensions are installed, you can connect to the PostgreSQL database:

```bash
psql -U USERNAME -d DBNAME
```

Within the PostgreSQL prompt, you can check for the installed extensions:

```sql
\dx
```

This command should display a list of installed extensions, including `earthdistance` and `cube`.


## Run as standalone application

1. Save your `default` and `aws` credentials in `tmp/config.ini`

    ```
    [default]
    POSTGRES_USER = LOCAL_USER
    POSTGRES_PASSWORD = password
    POSTGRES_HOST = localhost
    POSTGRES_PORT = PORT
    POSTGRES_DB = DBNAME

    [aws]
    POSTGRES_USER = AWS_USER
    POSTGRES_PASSWORD = password
    POSTGRES_HOST = dbname*.rds.amazonaws.com
    POSTGRES_PORT = AWS_PORT
    POSTGRES_DB = postgres
    ```
2. Set the appropriate values for S3_BUCKET and S3_DIR variables in `config.py`

3. Run the pipeline with the command 
```bash
python pipeline.py
```

## Run on Airflow

To run the pipeline using Airflow, start the Airflow web server and scheduler in separate terminals:

```bash
airflow webserver
airflow scheduler
```

Then, go to the Airflow UI at http://localhost:8080 and trigger the `weather_pipeline_dag` DAG.

