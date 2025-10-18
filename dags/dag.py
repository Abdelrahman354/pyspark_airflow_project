from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)
def my_dag():
    submit_job = SparkSubmitOperator(
        task_id="submit_job",
        conn_id="my_spark_conn",
        application="/usr/local/airflow/include/scripts/read.py",
        verbose=True,

        conf={
            "spark.submit.deployMode": "client"
        }
    )
    submit_job

my_dag()
