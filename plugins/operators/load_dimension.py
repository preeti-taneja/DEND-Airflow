from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    sql_template = '{}'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 query_nm="",
                 table = "",
                 delete_load = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.query_nm = query_nm
        self.table = table
        self.delete_load = delete_load

    def execute(self, context):
        self.log.info('LoadDimensionOperator -Process start')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.delete_load :
            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

        dim_sql = LoadDimensionOperator.sql_template.format(self.query_nm)
        #self.log.info("dim_sql".format(dim_sql))
        
        self.log.info('Run Fact Load Sql')
        redshift.run(dim_sql) 
