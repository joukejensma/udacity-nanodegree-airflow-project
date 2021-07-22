from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                redshift_conn_id,
                sql_stmt,
                destination_table,
                truncate_before_insert,
                *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sql_stmt = sql_stmt
        self.redshift_conn_id = redshift_conn_id        
        self.destination_table = destination_table
        self.truncate_before_insert = truncate_before_insert

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)    
        
        self.sql_stmt = f"INSERT INTO public.{self.destination_table} {self.sql_stmt}"
        if self.truncate_before_insert:
            self.sql_stmt = f"TRUNCATE public.{self.destination_table} {self.sql_stmt}"        
        self.log.info(f"Executing SQL query {self.sql_stmt}")
        redshift.run(self.sql_stmt)