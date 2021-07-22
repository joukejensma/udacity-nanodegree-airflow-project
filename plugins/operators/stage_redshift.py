from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """
    The stage operator is expected to be able to load any JSON formatted files from S3 to Amazon Redshift. The operator creates and runs a SQL COPY statement based on the parameters provided. The operator's parameters should specify where in S3 the file is loaded and what is the target table.

    The parameters should be used to distinguish between JSON file. Another important requirement of the stage operator is containing a templated field that allows it to load timestamped files from S3 based on the execution time and run backfills.
    """    
    
    ui_color = '#358140'
    
    # set up copy statement for staging
    template_fields = ("s3_key",)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
    """    
            #IGNOREHEADER {}
        #DELIMITER '{}'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,                 
        *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        self.log.info('Copy data from S3 to Redshift')
        
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # allow for backfilling
#         if ('log_data' in self.s3_key) | ('log-data' in self.s3_key):
#             execution_date = context.get('execution_date')
#             self.s3_key = f"log_data/{execution_date.year}/{execution_date.month}/*.json"
#             self.log.info(f"log_data found in s3_key path, changed path to {self.s3_key}!")
        
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(f"Specified s3 path is {s3_path}")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key
        )
        # self.ignore_headers,
        #             self.delimiter
        if self.table == 'staging_songs':
            formatted_sql += f"JSON 'auto'"            
        elif self.table == 'staging_events':
            formatted_sql += f"""
            IGNOREHEADER {self.ignore_headers}
            JSON 's3://{self.s3_bucket}/log_json_path.json'
            """
        
        self.log.info(f"Executing SQL query {formatted_sql}")
        redshift.run(formatted_sql)        
        
