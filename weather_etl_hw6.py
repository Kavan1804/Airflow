from datetime import datetime, timedelta
import os
import requests
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def get_logical_date():
    context = get_current_context()
    return str(context["logical_date"])[:10]


def get_next_day(date_str):
    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
    return (date_obj + timedelta(days=1)).strftime("%Y-%m-%d")


def return_snowflake_conn(conn_id):
    hook = SnowflakeHook(snowflake_conn_id=conn_id)
    conn = hook.get_conn()
    return conn.cursor()


def get_past_1_day_weather(date_str, latitude, longitude):
    url = "https://api.open-meteo.com/v1/forecast"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": date_str,
        "end_date": date_str,
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "precipitation_sum",
            "weather_code",
        ],
        "timezone": "America/Los_Angeles",
    }

    resp = requests.get(url, params=params, timeout=60)
    resp.raise_for_status()
    data = resp.json()

    df = pd.DataFrame(
        {
            "date": data["daily"]["time"],
            "temp_max": data["daily"]["temperature_2m_max"],
            "temp_min": data["daily"]["temperature_2m_min"],
            "precipitation": data["daily"]["precipitation_sum"],
            "weather_code": data["daily"]["weather_code"],
        }
    )

    df["date"] = pd.to_datetime(df["date"])
    return df


def save_weather_data(city, latitude, longitude, date_str, file_path):
    df = get_past_1_day_weather(date_str, latitude, longitude)
    df["city"] = city
    df.to_csv(file_path, index=False)


def populate_table_via_stage(cur, database, schema, table, file_path):
    stage_name = f"TEMP_STAGE_{table}"
    file_name = os.path.basename(file_path)

    cur.execute(f"USE SCHEMA {database}.{schema}")
    cur.execute(f"CREATE TEMPORARY STAGE {stage_name}")
    cur.execute(f"PUT file://{file_path} @{stage_name}")

    copy_query = f"""
        COPY INTO {schema}.{table}
        FROM @{stage_name}/{file_name}
        FILE_FORMAT = (
                TYPE = 'CSV'
                FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                SKIP_HEADER = 1
        )
    """
    cur.execute(copy_query)


@task
def extract(city, latitude, longitude):
    date_to_fetch = get_logical_date()

    file_path = f"/tmp/{city}_{date_to_fetch}.csv"
    save_weather_data(city, latitude, longitude, date_to_fetch, file_path)

    return {
        "file_path": file_path,
        "city": city,
        "date_to_fetch": date_to_fetch,
        "next_day": get_next_day(date_to_fetch),
    }


@task
def load(payload, database, schema, target_table, snowflake_conn_id):
    file_path = payload["file_path"]
    city = payload["city"].replace("'", "''")
    date_to_fetch = payload["date_to_fetch"]
    next_day_of_date_to_fetch = payload["next_day"]

    print(f"========= Updating {date_to_fetch}'s data for {city} ===========")
    cur = return_snowflake_conn(snowflake_conn_id)

    try:
        cur.execute("BEGIN;")
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS {database}.{schema}.{target_table} (
                date date,
                city varchar(32),
                temp_max float,
                temp_min float,
                precipitation float,
                weather_code int
            )"""
        )

        cur.execute(
            f"""DELETE FROM {database}.{schema}.{target_table}
                WHERE date >= '{date_to_fetch}'
                  AND date < '{next_day_of_date_to_fetch}'
                  AND city = '{city}'"""
        )

        populate_table_via_stage(cur, database, schema, target_table, file_path)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(e)
        raise


with DAG(
    dag_id="weather_ETL_incremental",
    start_date=datetime(2026, 2, 28),
    catchup=False,
    tags=["ETL"],
    schedule="30 3 * * *",
    max_active_runs=1,
) as dag:
    LATITUDE = float(Variable.get("LATITUDE", default_var="37.3382"))
    LONGITUDE = float(Variable.get("LONGITUDE", default_var="-121.8863"))
    CITY = Variable.get("CITY", default_var="San Jose")

    database = Variable.get("SNOWFLAKE_DATABASE", default_var="user_db_raccoon")
    schema = "raw"
    target_table = Variable.get("WEATHER_TABLE", default_var="city_weather")
    snowflake_conn_id = Variable.get("SNOWFLAKE_CONN_ID", default_var="snowflake_conn")

    payload = extract(CITY, LATITUDE, LONGITUDE)
    load(payload, database, schema, target_table, snowflake_conn_id)
