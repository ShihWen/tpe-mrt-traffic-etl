from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperatorMRT(BaseOperator):

    ui_color = '#80BD9E'
    
    truncate_sql = """
        TRUNCATE {table}
    """
    
    insert_sql = """
        {sql_query}
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket='',
                 s3_key='',
                 sql_statement='',
                 truncate_table=False,
                 *args, **kwargs):

        super(LoadDimensionOperatorMRT, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.sql_statement=sql_statement
        self.truncate_table=truncate_table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate_table:
            redshift.run(LoadDimensionOperatorMRT.truncate_sql.format(table=self.table))

        #version_id = redshift.get_records("SELECT DISTINCT version_id FROM staging_station")
        #self.log.info(f'version id of staging_station: {version_id}')
        exe_date = context['execution_date']
        formatted_insert_sql = LoadDimensionOperatorMRT.insert_sql.format(table=self.table
                                                                          , sql_query=self.sql_statement.format(year=exe_date.year, month=exe_date.month))
        redshift.run(formatted_insert_sql)
        total_rows = redshift.get_records(f"SELECT COUNT(*) FROM {self.table}")
        self.log.info(f'Total row of {self.table}: {total_rows}')
