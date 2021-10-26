from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql_transaction = """
        BEGIN;
        TRUNCATE {};
        INSERT INTO {} {};
        COMMIT;
    """

    @apply_defaults
    def __init__(self,
        redshift_conn_id='',
        table = '',
        select_sql='',
        *args, **kwargs
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = LoadDimensionOperator.insert_sql_transaction.format(
            self.table,
            self.table,
            self.select_sql
        )
        redshift.run(formatted_sql)
