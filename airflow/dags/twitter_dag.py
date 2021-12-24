import sys
sys.path.append("/home/afnscbrl/datapipeline/airflow/plugins/")

from datetime import datetime, date
from os.path import join
from pathlib import Path
from airflow.models import DAG
from operators.twitter_operator import TwitterOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
#from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from airflow.utils.dates import days_ago

ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(6)
}

BASE_FOLDER = join(
    str(Path("~/TwitterToSQL").expanduser()),
    "datalake/{stage}/bloomberg/{partition}"
)

PARTITION_FOLDER = "extract_date={{ ds }}"
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

with DAG(
    dag_id="twitter_dag",
    default_args=ARGS,
    schedule_interval="0 9 * * *", 
    max_active_runs=1
) as dag:
    twitter_operator = TwitterOperator(
        task_id="twitter_business",
        query="business",
        file_path=join(
            BASE_FOLDER.format(stage="Bronze", partition=PARTITION_FOLDER),
            "Bloomberg_{{ ds_nodash }}.json"
        ),
    )

    twitter_transform = SparkSubmitOperator(
            task_id="transform_twitter_business",
            application=join(
                str(Path(__file__).parents[2]),
                "spark/transformation.py"
            ),
            name = "twitter_transformation",
            application_args=[
                "--src",
                BASE_FOLDER.format(stage="Bronze", partition=PARTITION_FOLDER),
                "--dest",
                BASE_FOLDER.format(stage="Silver", partition=""),
                "--process-date",
                "{{ ds }}",
            ]
        )

twitter_operator >> twitter_transform