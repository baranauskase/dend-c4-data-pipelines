from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    truncate_insert_sql_transaction = """
        BEGIN;
        TRUNCATE {};
        INSERT INTO {} {};
        COMMIT;
    """
    insert_sql_transaction = """
        INSERT INTO {} {};
    """


    @apply_defaults
    def __init__(self,
        redshift_conn_id='',
        table = '',
        select_sql='',
        truncate_load_dim=True,
        *args, **kwargs
    ):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.select_sql = select_sql
        self.truncate_load_dim = truncate_load_dim

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Loading dimension table.')

        if self.truncate_load_dim:
            self.log.info(f'Constructing truncate insert query.')
            insert_sql = LoadDimensionOperator.truncate_insert_sql_transaction.format(
                self.table,
                self.table,
                self.select_sql
            )
        else:
            self.log.info(f'Constructing append insert query.')
            insert_sql = LoadDimensionOperator.insert_sql_transaction.format(
                self.table,
                self.select_sql
            )
        redshift.run(insert_sql)
