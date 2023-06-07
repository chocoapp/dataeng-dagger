from typing import Iterable, Mapping, Optional, Union

from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults
from dagger.dag_creator.airflow.operators.dagger_base_operator import DaggerBaseOperator


class PostgresOperator(DaggerBaseOperator):
    """
    Executes sql code in a specific Postgres database

    :param sql: the sql code to be executed. (templated)
    :type sql: Can receive a str representing a sql statement,
        a list of str (sql statements), or reference to a template file.
        Template reference are recognized by str ending in '.sql'
    :param postgres_conn_id: reference to a specific postgres database
    :type postgres_conn_id: str
    :param autocommit: if True, each command is automatically committed.
        (default value: False)
    :type autocommit: bool
    :param parameters: (optional) the parameters to render the SQL query with.
    :type parameters: mapping or iterable
    :param database: name of database which overwrite defined one in connection
    :type database: str
    """

    template_fields = ("sql", "params")
    template_ext = (".sql",)
    ui_color = "#ededed"

    @apply_defaults
    def __init__(
        self,
        sql: str,
        postgres_conn_id: str = "postgres_default",
        autocommit: bool = False,
        parameters: Optional[Union[Mapping, Iterable]] = None,
        database: Optional[str] = None,
        *args,
        **kwargs,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.sql = sql
        self.postgres_conn_id = postgres_conn_id
        self.autocommit = autocommit
        self.parameters = parameters
        self.database = database

        self._unpack_columns()

    def _unpack_columns(self):
        """
        Unpacks columns from parameters dict into a string
        """
        # if parameters exists and is dict and has columns
        if (
            self.parameters
            and isinstance(self.parameters, dict)
            and "columns" in self.parameters):
                self.parameters["columns"] = ", ".join([f'"{column}"' for column in self.parameters["columns"]])

    def execute(self, context):
        self.log.info("Executing: %s", self.sql)
        self.hook = PostgresHook(
            postgres_conn_id=self.postgres_conn_id, schema=self.database
        )
        self.hook.run(self.sql, self.autocommit, parameters=self.parameters)
        for output in self.hook.conn.notices:
            self.log.info(output)
