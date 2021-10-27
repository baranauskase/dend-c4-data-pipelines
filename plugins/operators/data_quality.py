import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
        redshift_conn_id='',
        qa_checks=[],
        *args, **kwargs
    ):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.qa_checks = qa_checks,

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for q in self.qa_checks[0]:
            records = redshift.get_records(q)
            if len(records) != 1:
                raise ValueError(f'Data quality check failed. Received {len(records)} rows for query: {q}')
            
            for rec in records:
                for val in rec:
                    if not bool(val):
                        raise ValueError(f'Data quality check failed. Bool eval failed for query: {q}')

            self.log.info(f'Data quality passed for query: {q}')