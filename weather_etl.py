from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import timedelta
from datetime import datetime
import requests


default_args = {
    "owner": "Kavan",
    "email": ["kavan.siddeshkumar@sjsu.edu"],
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}


def return_snowflake_conn(con_id):
    hook = SnowflakeHook(snowflake_conn_id=con_id)
    conn = hook.get_conn()
    return conn.cursor()


@task
def extract(latitude, longitude):
    """Get the past 60 days of weather for a given pair of coordinates"""

    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "past_days": 60,
        "forecast_days": 0,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "weather_code",
        ],
        "timezone": "Asia/Kolkata",
    }

    response = requests.get(url, params=params, timeout=60)

    if response.status_code != 200:
        raise RuntimeError(f"API request failed: {response.status_code}")

    return response.json()


@task
def transform(raw_data, latitude, longitude, city):
    if "daily" not in raw_data:
        raise ValueError("Missing 'daily' key in API response")

    data = raw_data["daily"]

    records = []
    for i in range(len(data["time"])):
        records.append(
            {
                "latitude": float(latitude),
                "longitude": float(longitude),
                "date": data["time"][i],
                "temp_max": data["temperature_2m_max"][i],
                "temp_min": data["temperature_2m_min"][i],
                "weather_code": str(data["weather_code"][i]),
                "city": city,
            }
        )

    return records


@task
def load(con, target_table, records):
    try:
        con.execute("BEGIN;")

        con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                latitude FLOAT,
                longitude FLOAT,
                date DATE,
                temp_max FLOAT,
                temp_min FLOAT,
                weather_code VARCHAR(3),
                city VARCHAR(100),
                PRIMARY KEY (latitude, longitude, date, city)
            );
            """
        )

        con.execute(f"DELETE FROM {target_table};")

        insert_sql = f"""
            INSERT INTO {target_table} (
                latitude, longitude, date, temp_max, temp_min, weather_code, city
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """

        data = [
            (
                r["latitude"],
                r["longitude"],
                r["date"],
                r["temp_max"],
                r["temp_min"],
                r["weather_code"],
                r["city"],
            )
            for r in records
        ]

        con.executemany(insert_sql, data)

        con.execute("COMMIT;")
        print(f"Loaded {len(records)} records into {target_table}")

    except Exception as e:
        con.execute("ROLLBACK;")
        print(e)
        raise


with DAG(
    dag_id="weather_etl",
    start_date=datetime(2026, 3, 4),
    catchup=False,
    tags=["ETL"],
    default_args=default_args,
    schedule="30 2 * * *",
) as dag:

    LATITUDE = Variable.get("LATITUDE")
    LONGITUDE = Variable.get("LONGITUDE")
    CITY = "Bengaluru"

    target_table = "raw.assignment5_weather_data"
    cur = return_snowflake_conn("snowflake_conn")

    raw_data = extract(LATITUDE, LONGITUDE)
    data = transform(raw_data, LATITUDE, LONGITUDE, CITY)
    load(cur, target_table, data)
