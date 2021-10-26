from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class RunDDLOperator(BaseOperator):

    ui_color = '#80BD9E'
    ddl_sql_transaction = """
        BEGIN;
        {}
        COMMIT;
    """

    @apply_defaults
    def __init__(self,
        redshift_conn_id='',
        ddl_sql='',
        *args, **kwargs
    ):
        super(RunDDLOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.ddl_sql = ddl_sql

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql = RunDDLOperator.ddl_sql_transaction.format(
            self.ddl_sql
        )
        redshift.run(formatted_sql)
