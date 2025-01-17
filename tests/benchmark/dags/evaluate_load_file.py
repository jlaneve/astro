import json
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG

from astro import sql as aql
from astro.sql.table import Table

START_DATE = datetime(2000, 1, 1)


def load_config():
    config_path = Path(Path(__file__).parent.parent, "config.json").resolve()
    with open(config_path) as fp:
        return json.load(fp)


@aql.transform
def count_rows(input_table: Table):
    return """
        SELECT COUNT(*)
        FROM {input_table}
    """


@aql.transform
def count_columns(input_table: Table):
    return """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_name={input_table};
    """


def create_dag(database_name, table_args, dataset):
    dataset_name = dataset["name"]
    dataset_path = dataset["path"]
    # dataset_rows = dataset["rows"]

    dag_name = f"load_file_{dataset_name}_into_{database_name}"
    table_name = Path(dataset_path).stem

    with DAG(dag_name, schedule_interval=None, start_date=START_DATE) as dag:
        chunk_size = int(os.environ["ASTRO_CHUNKSIZE"])
        table_metadata = Table(table_name=table_name, **table_args)
        table_xcom = aql.load_file(  # noqa: F841
            path=dataset_path,
            task_id="load_csv",
            output_table=table_metadata,
            chunksize=chunk_size,
        )

        # Todo: Check is broken so the following code is commented out, uncomment when fixed
        # aggregate_check(
        #    table_xcom,
        #    check="SELECT COUNT(*) FROM {table}",
        #    equal_to=dataset_rows
        # )
        # table_xcom.set_downstream(aggregate_check)

        return dag


config = load_config()
for database in config["databases"]:
    database_name = database["name"]
    table_args = database["params"]
    for dataset in config["datasets"]:
        dag = create_dag(database_name, table_args, dataset)
        globals()[dag.dag_id] = dag
