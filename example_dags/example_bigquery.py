import pandas as pd
from airflow.decorators import task
from airflow.models.dag import DAG
from airflow.utils import timezone

import astro.sql as aql
from astro.sql.table import Table

with DAG(
    dag_id="example_google_bigquery_gcs_load_and_save",
    schedule_interval=None,
    start_date=timezone.datetime(2022, 1, 1),
) as dag:
    t1 = aql.load_file(
        task_id="load_from_github_to_bq",
        path="https://raw.githubusercontent.com/astro-projects/astro/main/tests/data/imdb.csv",
        # export AIRFLOW_CONN_BQ_TABLE="bigquery://astronomer-dag-authoring"
        output_table=Table("imdb_movies", conn_id="bq_table", database="astro"),
    )

    @task
    def extract_top_5_movies(input_df: pd.DataFrame):
        print(f"Total Number of records: {len(input_df)}")
        top_5_movies = input_df.sort_values(by="imdb_score", ascending=False).head(5)
        print(f"Top 5 Movies: {top_5_movies}")
        return top_5_movies

    t2 = extract_top_5_movies(input_df=t1)

    aql.save_file(
        task_id="save_to_gcs",
        input=t2,
        output_file_path="gs://dag-authoring/{{ task_instance_key_str }}/homes.csv",
        output_file_format="csv",
        output_conn_id="google_cloud_default",
        overwrite=True,
    )
