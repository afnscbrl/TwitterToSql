import sys
sys.path.append("/home/afnscbrl/Documents/TwitterToSQL/airflow/plugins")
from datetime import datetime
from os.path import join
from airflow.models import DAG
from operators.twitter_operator import TwitterOperator

with DAG(dag_id="twitter_dag", schedule_interval='@daily', start_date=datetime.now()) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_bloomberg",
        query="business",
        file_path=join(
            "/home/afnscbrl/Documents/TwitterToSQL/datalake",
            "bloomberg",
            "extract_date-{{ ds }}",
            "Bloomberg_{{ ds_nodash }}.json"
        )
    )