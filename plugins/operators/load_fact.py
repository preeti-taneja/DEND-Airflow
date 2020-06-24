from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    sql_template = '{}'
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 query_nm="",
                 table = "",
                 delete_load = False,
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query_nm = query_nm
        self.table = table
        self.delete_load = delete_load

    def execute(self, context):
        self.log.info('LoadFactOperator -Process Started')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info('Load data in fact table')
        
        if self.delete_load :
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))
            
        fact_sql = LoadFactOperator.sql_template.format(self.query_nm)
        #self.log.info("fact_sql".format(fact_sql))
      
        self.log.info('Run Fact Load Sql')
        redshift.run(fact_sql) 
