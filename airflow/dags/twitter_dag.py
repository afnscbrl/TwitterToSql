import sys
sys.path.append("/home/afnscbrl/Documents/TwitterToSQL/airflow/plugins/")

from datetime import datetime, date
from os.path import join
from pathlib import Path
from airflow.models import DAG
from operators.twitter_operator import TwitterOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.dates import days_ago


#Defining arguments to dag
ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now()
}

#Defining folder to store data lake
BASE_FOLDER = join(
    # str(Path("~/TwitterToSQL").expanduser()),
    # "datalake/{stage}/bloomberg/{partition}"
    str(Path("~/Documents").expanduser()),
    "TwitterToSQL/datalake/{stage}/bloomberg/{partition}"
)

PARTITION_FOLDER = "extract_date={{ ds }}"
#TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.00Z"

#Creating a dag
with DAG(
    dag_id="twitter_dag",
    default_args=ARGS,
    schedule_interval="0 9 * * *", 
    max_active_runs=1
) as dag:
    #Creating a dag task. This task get the methods on twitter_operator.py that get tweets from API
    twitter_operator = TwitterOperator(
        task_id="twitter_business",
        query="business",
        file_path=join(
            BASE_FOLDER.format(stage="Bronze", partition=PARTITION_FOLDER),
            "Bloomberg_{{ ds_nodash }}.json"
        ),
    )

    #Creating a dag task. This task get the methods on transformation.py that process the data in Bronze 
        #stage data lake to Silver stage data lake
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
    twitter_gold = SparkSubmitOperator(
            task_id="transform_twitter_gold",
            application=join(
                str(Path(__file__).parents[2]),
                "spark/insight_tweet.py"
            ),
            name = "tweets_to_gold"
        )

#This define the hierarchy of the tasks.
twitter_operator >> twitter_transform >> twitter_gold