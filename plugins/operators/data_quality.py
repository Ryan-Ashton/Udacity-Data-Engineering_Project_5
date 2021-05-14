from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Reference
# Exercise 1: Operator Plugins


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        
        for q in self.dq_checks:
            if redshift_hook.get_records(q['check_sql']) != q['expected_result']:
                raise ValueError("Data quality check failed. Returned no results")
            else:
                self.log.info("Data quality check on table passed")
            
        