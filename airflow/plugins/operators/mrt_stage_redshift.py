from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



class StageToRedshiftOperatorMRT(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    exe_sql = """       
            COPY {staging_table}
            FROM '{data_path}'
            ACCESS_KEY_ID '{access_key}'
            SECRET_ACCESS_KEY '{secret_key}'
            REGION '{region}'
            CSV
            IGNOREHEADER 1        
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 *args, **kwargs):

        super(StageToRedshiftOperatorMRT, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.region = region

    def execute(self, context):
        self.log.info(f'Starting to copy staging table: {self.table}')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
     
        if self.table in ['staging_traffic']:
            func = lambda x: '{0:0>2}'.format(x)
            exe_date = context['execution_date']

            file_key = f'mrt_traffic_{exe_date.year}{func(exe_date.month)}.csv'
            self.log.info(f'Finish copying staging table: {self.s3_key}')
            self.log.info(f'context: {exe_date.year}, {func(exe_date.month)}')
            s3_path = "{}/{}/{}".format(self.s3_bucket,self.s3_key ,file_key)
            
            
            formatted_sql = StageToRedshiftOperatorMRT.exe_sql.format(
                staging_table=self.table,
                data_path=s3_path,
                access_key=credentials.access_key,
                secret_key=credentials.secret_key,
                region=self.region
            )

            redshift.run(formatted_sql)
            self.log.info(f'Finish copying staging table: {self.table}')
        
        elif self.table in ['staging_station' , 'staging_station_exit']: 
            rendered_key = self.s3_key.format(**context)
            s3_path = "{}/{}".format(self.s3_bucket, rendered_key)
            self.log.info(f'rendered_key is: {rendered_key}')

            if redshift.get_records(f"SELECT COUNT(*) FROM {self.table}")[0][0]==0:
            #redshift.run(f"TRUNCATE {self.table}")
                self.log.info(f'{self.table} is empty, copying data')
                formatted_sql = StageToRedshiftOperatorMRT.exe_sql.format(
                    staging_table=self.table,
                    data_path=s3_path,
                    access_key=credentials.access_key,
                    secret_key=credentials.secret_key,
                    region=self.region
                )

                redshift.run(formatted_sql)
                self.log.info(f'Finish copying staging table: {self.table}')
                total_rows = redshift.get_records(f"SELECT COUNT(*) FROM {self.table}")
                self.log.info(f'Total row of {self.table}: {total_rows}')
     
        
 
