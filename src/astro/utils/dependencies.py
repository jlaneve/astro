from astro import constants


class MissingPackage:
    def __init__(self, module_name, related_extras):
        self.module_name = module_name
        self.related_extras = related_extras

    def __getattr__(self, item):
        raise RuntimeError(
            f"Error loading the module {self.module_name},"
            f" please make sure all the dependencies are installed."
            f" try - pip install {constants.PYPI_PROJECT_NAME}[{self.related_extras}]"
        )


try:
    from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
except ModuleNotFoundError:
    BigQueryHook = MissingPackage(
        "airflow.providers.google.cloud.hooks.bigquery", "google"
    )

try:
    from airflow.providers.postgres.hooks.postgres import PostgresHook
except ModuleNotFoundError:
    PostgresHook = MissingPackage(
        "airflow.providers.postgres.hooks.postgres", "postgres"
    )

try:
    from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
except ModuleNotFoundError:
    SnowflakeHook = MissingPackage(
        "airflow.providers.snowflake.hooks.snowflake", "snowflake"
    )

try:
    from snowflake.connector import pandas_tools
except ModuleNotFoundError:
    pandas_tools = MissingPackage("snowflake-connector-python[pandas]", "postgres")

try:
    from boto3 import Session as BotoSession
except ModuleNotFoundError:
    BotoSession = MissingPackage("s3fs", "amazon")

try:
    from google.cloud.storage import Client as GCSClient
except ModuleNotFoundError:
    GCSClient = MissingPackage("apache-airflow-providers-google", "google")

try:
    from google.oauth2 import service_account as google_service_account
except ModuleNotFoundError:
    google_service_account = MissingPackage("apache-airflow-providers-google", "google")

try:
    from psycopg2 import sql as postgres_sql
except ModuleNotFoundError:
    postgres_sql = MissingPackage("psycopg2", "postgres")

try:
    from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
except ModuleNotFoundError:
    AwsBaseHook = MissingPackage("apache-airflow-providers-amazon", "amazon")

try:
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
except ModuleNotFoundError:
    GCSHook = MissingPackage("apache-airflow-providers-google", "google")

try:
    from airflow.providers.amazon.aws.hooks import s3
except ModuleNotFoundError:
    s3 = MissingPackage("apache-airflow-providers-amazon", "amazon")

try:
    from airflow.providers.google.cloud.hooks import gcs
except ModuleNotFoundError:
    gcs = MissingPackage("apache-airflow-providers-google", "google")
