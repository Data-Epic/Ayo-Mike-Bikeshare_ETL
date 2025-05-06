from airflow.decorators import dag, task
from zipfile import ZipFile
from io import TextIOWrapper
import os
import pandas as pd
import time
import pendulum
import plotly.express as px 
import logging

default_args = {
    "owner": "leke",
    "start_date": pendulum.datetime(2024, 5, 1, tz="UTC"),
    "retries": 5,
    "retry_delay": pendulum.duration(minutes=2),
}

@dag(
    dag_id="bikeshare_etl_v4",
    default_args=default_args,
    schedule="0 10 * * 1",
    catchup=False,
)
def bikeshare_etl():
    @task
    def file_information():
        return {
            "filepath": "/opt/airflow/dags/202212-capitalbikeshare-tripdata.zip",
            "csv_name": "202212-capitalbikeshare-tripdata.csv",
        }

    @task
    def read_csv_to_parquet(file_info_dict):
        filepath = file_info_dict["filepath"]
        csv_name = file_info_dict["csv_name"]
        output_path = "/opt/airflow/dags/data/bikeshare.parquet"

        try:
            with ZipFile(filepath, "r") as unzip_it:
                with unzip_it.open(csv_name) as csv_file:
                    df = pd.read_csv(TextIOWrapper(csv_file, encoding="UTF-8"))

                    os.makedirs(os.path.dirname(output_path), exist_ok=True)  
                    df.to_parquet(output_path, index=False)
                    return output_path
        except KeyError:
            raise ValueError(f"Error: '{csv_name}' not found in '{filepath}'")
        except FileNotFoundError:
            raise ValueError(f"Error: ZIP file not found at '{filepath}'")
        except Exception as e:
            raise RuntimeError(f"An error occurred: {e}")

    @task
    def partition_to_parquet(parquet_path: str):
        df = pd.read_parquet(parquet_path)
        df['started_at'] = pd.to_datetime(df['started_at'])
        df['ended_at'] = pd.to_datetime(df['ended_at'])
        df['week'] = df['started_at'].dt.isocalendar().week
        df['duration'] = (df['ended_at'] - df['started_at']).dt.total_seconds()

        for member_type in df['member_casual'].unique():
            for week in df['week'].unique():
                subset = df[(df['member_casual'] == member_type) & (df['week'] == week)]
                path = f"/opt/airflow/dags/output/{member_type}/week_{week}/"
                os.makedirs(path, exist_ok=True)

                file_path = os.path.join(path, "data.parquet")
                subset.to_parquet(file_path, index=False)

    
    @task
    def generate_heatmap(parquet_path: str):
        df = pd.read_parquet(parquet_path)
        df = df.dropna(subset=["start_lat", "start_lng"])


        fig = px.density_map(
                df,
                lat="start_lat",
                lon="start_lng",
                radius=10,
                center=dict(lat=df["start_lat"].mean(), lon=df["start_lng"].mean()),
                zoom=11,
                map_style="carto-positron",  # No Mapbox token needed
                title="Bikeshare Start Location Heatmap"
            )

        output_dir = "/opt/airflow/dags/heatmap"
        os.makedirs(output_dir, exist_ok=True)
        fig.write_html(os.path.join(output_dir, "heatmap.html"))
    

    @task
    def stream_it(parquet_path: str):
        logger = logging.getLogger(__name__)
        df = pd.read_parquet(parquet_path)
        df['started_at'] = pd.to_datetime(df['started_at'])
        df['ended_at'] = pd.to_datetime(df['ended_at'])
        df['duration'] = (df['ended_at'] - df['started_at']).dt.total_seconds()

        def trip_stream(df_):
            for _, row in df_.iterrows():
                yield row
                time.sleep(0.5)

        for trip in trip_stream(df):
            duration_minutes = trip['duration'] / 60
            start_hour = trip['started_at'].hour

            if duration_minutes > 45:
                logger.warning(f"⚠️ ALERT: Long ride detected! Duration: {duration_minutes:.2f} minutes")
            if trip['member_casual'] == 'casual' and start_hour == 0:
                logger.warning("⚠️ ALERT: Casual rider started a trip at midnight!")

    # DAG execution
    file_info = file_information()
    parquet_path = read_csv_to_parquet(file_info)
    partition_to_parquet(parquet_path)
    generate_heatmap(parquet_path)
    stream_it(parquet_path)


bikeshare_etl = bikeshare_etl()