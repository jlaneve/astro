from typing import Dict

from airflow.decorators.base import get_unique_task_id

from astro.sql.operators.sql_decorator import SqlDecoratoratedOperator
from astro.sql.table import Table


class SqlTruncateOperator(SqlDecoratoratedOperator):
    def __init__(
        self,
        table: Table,
        **kwargs,
    ):
        self.sql = ""
        self.table = table

        task_id = get_unique_task_id(table.table_name + "_truncate")

        def null_function(table: Table):
            pass

        super().__init__(
            raw_sql=True,
            parameters={},
            task_id=task_id,
            op_args=(),
            op_kwargs={"table": table},
            python_callable=null_function,
            **kwargs,
        )

    def execute(self, context: Dict):
        def table_func(table: Table):
            return "TRUNCATE TABLE {table}"

        self.python_callable = table_func
        self.parameters = {"table_name": self.table.table_name}

        super().execute(context)
