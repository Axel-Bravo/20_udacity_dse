import logging
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class S3ToRedshiftOperator(BaseOperator):
    @apply_defaults
    def __init__(self,
                 redshift_conn_id: str = "",
                 aws_credentials_id: str = "",
                 table: str = "",
                 s3_bucket: str = "",
                 s3_key: str = "",
                 delimiter: str = ",",
                 ignore_headers: int = 1,
                 *args,
                 **kwargs):

        super().__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        self.log.info("Clearing data from destination Redshift table")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(f"DELETE FROM {self.table}")

        self.log.info("Copying data from S3 to Redshift")
        s3_path = f"s3://{self.s3_bucket}/{self.s3_key.format(**context)}"

        sql_query = f"""
                        COPY {self.table}
                        FROM '{s3_path}'
                        ACCESS_KEY_ID '{credentials.access_key}'
                        SECRET_ACCESS_KEY '{credentials.secret_key}'
                        IGNOREHEADER {self.ignore_headers}
                        DELIMITER '{self.delimiter}'
                    """
        redshift.run(sql_query)
