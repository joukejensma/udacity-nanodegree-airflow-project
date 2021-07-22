from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                redshift_conn_id,
                sql_stmt,
                destination_table,
                *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql_stmt = sql_stmt
        self.redshift_conn_id = redshift_conn_id
        self.destination_table = destination_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)    
        
        self.sql_stmt = f"INSERT INTO public.{self.destination_table} {self.sql_stmt}"        
        self.log.info(f"Executing SQL query {self.sql_stmt}")
        redshift.run(self.sql_stmt)      
