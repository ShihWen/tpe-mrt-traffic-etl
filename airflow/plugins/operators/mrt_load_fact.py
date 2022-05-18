from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers.mrt_sql_queries import MrtSqlQueries

class LoadFactOperatorMRT(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 *args, **kwargs):

        super(LoadFactOperatorMRT, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.table=table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        traffic_row = redshift.get_records(f"SELECT COUNT(*) FROM staging_traffic")
        station_dim_row = redshift.get_records(f"SELECT COUNT(*) FROM mrt_station_dim")
        time_dim_row = redshift.get_records(f"SELECT COUNT(*) FROM time_dim")
        self.log.info(f'staging_traffic rows: {traffic_row}')
        self.log.info(f'station_dim rows: {station_dim_row}')
        self.log.info(f'time_dim rows: {time_dim_row}')
        
        exe_date = context['execution_date']
        
        formatted_sql = MrtSqlQueries.insert_traffic_fact.format(year=exe_date.year, month=exe_date.month)
        redshift.run(formatted_sql)

