from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 sql_query: str = "",
                 table: str = "",
                 delete_load: bool = False,
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query
        self.table = table
        self.delete_load = delete_load
        self.delete_load_sql = f"DROP TABLE IF EXISTS {self.table}" if delete_load else ""

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Start - Load Dimensional table")

        if self.delete_load:
            self.log.debug("Load Dimensional table - Delete-load mode")
            redshift.run(self.delete_load_sql)
        else:
            self.log.debug("Load Dimensional table - Append-load mode")

        redshift.run(self.sql_query)
        self.log.info("End - Load Dimensional table")
