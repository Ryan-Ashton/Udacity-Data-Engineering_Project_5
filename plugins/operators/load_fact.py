from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Reference
# Exercise 1: Operator Plugins


class LoadFactOperator(BaseOperator):
    insert_sql = """
        INSERT INTO {}
        {}
    """

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 select_query="",
                 append_data = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.select_query = select_query
        self.redshift_conn_id = redshift_conn_id
        self.append_data = append_data

    def execute(self, context):
        self.log.info(f"Start LoadFactOperator for {self.table}")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        #https://knowledge.udacity.com/questions/65402
        
        if not self.append_data:
            self.log.info("Clearing data from Destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from Staging table to Fact table")
        load_data_sql = LoadFactOperator.insert_sql.format(
            self.table,
            self.select_query
        )
        redshift.run(load_data_sql)
