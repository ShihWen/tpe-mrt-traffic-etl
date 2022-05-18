from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperatorMRT(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperatorMRT, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.dq_checks=dq_checks

    def execute(self, context):

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        error_count = 0
        failing_tests = []
        self.log.info('Starting to Data Quality checks')
        
        for check in self.dq_checks:
            sql = check.get('check_sql')
            
            exe_date = context['execution_date']
            formatted_sql = sql.format(year=exe_date.year, month=exe_date.month, day=exe_date.day)
            
            exp_sql = check.get('expected_sql')          
            
            check = redshift.get_records(formatted_sql)[0][0]
            expect = redshift.get_records(exp_sql)[0][0]
            
            
            self.log.info(f"check result:{redshift.get_records(formatted_sql)}")
            self.log.info(f"expected result:{redshift.get_records(exp_sql)}")
            
            if check != expect:
                error_count += 1
                failing_tests.append(formatted_sql)
         
        if error_count > 0:
            self.log.info('Test failed')
            self.log.info(failing_tests)
            raise ValueError('Data Quality check failed')
        
        self.log.info('Data quality checks passed!')

            