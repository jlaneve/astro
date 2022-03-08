import pathlib

from airflow.decorators import task_group
from airflow.utils import timezone
from astro import sql as aql
from astro.sql.table import Table, TempTable
from astro.dataframe import dataframe as adf
from tests.operators import utils as test_utils
import pandas as pd
import math
DEFAULT_DATE = timezone.datetime(2016, 1, 1)

CWD = pathlib.Path(__file__).parent
import pytest


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}


def merge_keys(sql_server, mode):
    """
    To match with their respective API's, we have a slightly different "merge_keys" value
    when a user is using snowflake.
    :param sql_server:
    :return:
    """
    sql_name, _ = sql_server
    keys = []
    if mode == "single":
        keys = ["list"]
    if mode == "multi":
        keys = ["list", "sell"]
    if mode == "mixed":
        keys = ["list", "sell"]

    if sql_name == "snowflake":
        return {k: k for k in keys}
    else:
        return keys

@pytest.fixture
def merge_parameters(request, sql_server):
    mode = request.param
    if mode == "single":
        return ({
            "merge_keys": merge_keys(sql_server, mode),
            "merge_columns": ["list"],
            "target_columns": ["list"],
            "conflict_strategy": "ignore"
        }, mode)
    elif mode == "multi":
        return ({
            "merge_keys": merge_keys(sql_server, mode),
            "merge_columns": ["list", "sell"],
            "target_columns": ["list", "sell"],
            "conflict_strategy": "ignore"
        }, mode)
    elif mode == "mixed":
        return ({
            "merge_keys": merge_keys(sql_server, mode),
            "merge_columns": ["list", "sell", "taxes"],
            "target_columns": ["list", "sell", "age"],
            "conflict_strategy": "update"
        }, mode)



@aql.transform
def do_a_thing(input_table: Table):
    return "SELECT * FROM {{input_table}}"


@aql.run_raw_sql
def add_constraint(table: Table, columns):
    if table.conn_type == "sqlite":
        return "CREATE UNIQUE INDEX unique_index ON {{table}}" + f"({','.join(columns)})"
    return "ALTER TABLE {{table}} ADD CONSTRAINT airflow UNIQUE" + f" ({','.join(columns)})"

@adf
def validate_results(df: pd.DataFrame, mode):
    if mode == "single":
        assert df.age.to_list()[:-1] == [60.0, 12.0, 41.0, 22.0]
        assert math.isnan(df.age.to_list()[-1])
        assert df.taxes.to_list()[:-1] == [3167.0, 4033.0, 1471.0, 3204.0]
        assert math.isnan(df.taxes.to_list()[-1])
        assert df.taxes.to_list()[:-1] == [3167.0, 4033.0, 1471.0, 3204.0]
        assert df.list.to_list() == [160, 180, 132, 140, 240]
        assert df.sell.to_list()[:-1] == [142, 175, 129, 138]
        assert math.isnan(df.taxes.to_list()[-1])
    elif mode == "multi":
        assert df.age.to_list()[:-1] == [60.0, 12.0, 41.0, 22.0]
        assert math.isnan(df.age.to_list()[-1])
        assert df.taxes.to_list()[:-1] == [3167.0, 4033.0, 1471.0, 3204.0]
        assert math.isnan(df.taxes.to_list()[-1])
        assert df.taxes.to_list()[:-1] == [3167.0, 4033.0, 1471.0, 3204.0]
        assert df.list.to_list() == [160, 180, 132, 140, 240]
        assert df.sell.to_list() == [142, 175, 129, 138, 232]
    elif mode == "mixed":
        assert df.taxes.to_list() == [1, 1, 1, 1, 1]
        assert df.age.to_list()[:-1] == [60.0, 12.0, 41.0, 22.0]
        assert math.isnan(df.age.to_list()[-1])

@task_group
def run_merge(output_specs: TempTable, merge_parameters, mode):
    main_table = aql.load_file(
        path=str(CWD) + "/../data/homes_merge_1.csv",
        output_table=output_specs,
    )
    merge_table = aql.load_file(
        path=str(CWD) + "/../data/homes_merge_2.csv",
        output_table=output_specs,
    )

    con1 = add_constraint(main_table, merge_parameters["merge_keys"])

    merged_table = aql.merge(
        target_table=main_table,
        merge_table=merge_table,
        **merge_parameters,
    )
    con1 >> merged_table
    validate_results(df=merged_table, mode=mode)

@pytest.mark.parametrize(
    "sql_server",
    [
        # "snowflake",
        "postgres",
        # pytest.param("bigquery", marks=pytest.mark.xfail(reason="some bug")),
        "sqlite",
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "merge_parameters", [
        # "None",
        "single",
        "multi",
        "mixed"
    ],
    indirect=True
)
def test_merge(sql_server, sample_dag, tmp_table, merge_parameters):
    merge_params, mode = merge_parameters
    with sample_dag:
        output_table = tmp_table
        run_merge(output_table, merge_params, mode)
    test_utils.run_dag(sample_dag)