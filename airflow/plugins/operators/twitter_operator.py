import sys
sys.path.append("/home/afnscbrl/Documents/TwitterToSQL/airflow/plugins")


import json
from datetime import datetime
from pathlib import Path
from os.path import join

from airflow.models import DAG, BaseOperator
from airflow.utils.decorators import apply_defaults
from hooks.twitter_hook import TwitterHook

#This class get a request made for Twitter_Hook and transform into airflow operator,
# the, save the resquest into a json file.
class TwitterOperator(BaseOperator):

    template_fields = [
        "query",
        "file_path"
    ]

    def __init__(
        self,
        query,
        file_path,
        conn_id = None,
        *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.query = query
        self.file_path = file_path
        self.conn_id = conn_id

    def create_parent_folder(self):
        Path(Path(self.file_path).parent).mkdir(parents=True, exist_ok=True)

    def execute(self, context):
        hook = TwitterHook(
            query=self.query,
            conn_id=self.conn_id
        )
        self.create_parent_folder()
        with open(self.file_path, "w") as output_file:
            for pg in hook.run():
                json.dump(pg, output_file, ensure_ascii=False)
                output_file.write("\n")

if __name__ == "__main__":
    #Only to test
    with DAG(dag_id="TwitterTest", start_date=datetime.now()) as dag:
        to = TwitterOperator(
            query="business",
            file_path=join(
                "/home/afnscbrl/Documents/TwitterToSQL/datalake",
                "bloomberg",
                "extract_date-{{ ds }}",
                "Bloomberg_{{ ds_nodash }}.json"
                ),
            task_id="test_run"
        )
        to.run()