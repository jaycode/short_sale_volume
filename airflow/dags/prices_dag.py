from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator,
                               PostgresOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'jaycode',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup':False
}

dag = DAG('prices_dag',
          default_args=default_args,
          description="Pull stock pricing data from Quotemedia",
          schedule_interval='@hourly',
          max_active_runs=1
        )

wait_for_cluster >> create_spark_session >> \
pull_stock_symbols >> pull_short_interest_data