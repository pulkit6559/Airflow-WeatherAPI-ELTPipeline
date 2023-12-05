
select_query = "SELECT {columns} FROM {table} WHERE {condition}"

update_query = "UPDATE {table} SET {set_clause} WHERE {condition}"

insert_query = "INSERT INTO {table} ({columns}) VALUES ({values})"

delete_query = "DELETE FROM {table} WHERE {condition}"

monthly_avg_by_station = """
SELECT
    {table}.station_ID,
    EXTRACT(MONTH FROM date) AS month,
    AVG(value) AS avg_temperature
FROM
    {table}
WHERE
    EXTRACT(YEAR FROM date) BETWEEN {start_year} AND {end_year}
GROUP BY
    station_id, EXTRACT(MONTH FROM date)
"""

yearly_max_by_station = """
SELECT
    station_id,
    EXTRACT(YEAR FROM date) AS year,
    MAX(value) AS max_temperature
FROM
    TMAX
GROUP BY
    station_id, EXTRACT(YEAR FROM date)
"""

extreme_temp_days_by_station = """
SELECT
    station_id,
    date,
    CASE
        WHEN value >= (SELECT AVG(value) FROM TMAX) THEN 'Above Average'
        ELSE 'Below Average'
    END AS temperature_category
FROM
    TMAX
WHERE
    EXTRACT(YEAR FROM date) BETWEEN 1990 AND 2000
"""

temp_trends_by_station = """
SELECT
    station_id,
    date,
    AVG(value) OVER (PARTITION BY station_id ORDER BY date) AS moving_avg_temperature
FROM
    TMAX
WHERE
    EXTRACT(YEAR FROM date) BETWEEN 1990 AND 2000
"""

monthly_temp_variation = """
SELECT
    station_id,
    EXTRACT(MONTH FROM date) AS month,
    MAX(value) - MIN(value) AS temperature_variation
FROM
    TMAX
WHERE
    EXTRACT(YEAR FROM date) BETWEEN 1990 AND 2000
GROUP BY
    station_id, EXTRACT(MONTH FROM date)
"""

temp_anomalies = """
SELECT
    station_id,
    date,
    CASE
        WHEN value > (SELECT MAX(value) FROM TMAX WHERE EXTRACT(YEAR FROM date) BETWEEN 1990 AND 2000) THEN 'Above Normal'
        WHEN value < (SELECT MIN(value) FROM TMIN WHERE EXTRACT(YEAR FROM date) BETWEEN 1990 AND 2000) THEN 'Below Normal'
        ELSE 'Normal'
    END AS temperature_pattern
FROM
    TMAX
WHERE
    EXTRACT(YEAR FROM date) BETWEEN 1990 AND 2000
"""
