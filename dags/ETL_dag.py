from airflow.decorators import dag
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["bronze", "silver", "gold"],
)
def ETL_dag():
    # -----Bronze Layer Job ----
    bronze_job = SparkSubmitOperator(
        task_id="bronze_job",
        conn_id="my_spark_conn",
        application="/usr/local/airflow/include/scripts/bronze.py",
        verbose=True,
        conf={
            "spark.submit.deployMode": "client"
        }
    )
    
    # ---- Silver Layer Job -----
    silver_job = SparkSubmitOperator(
        task_id="silver_job",
        conn_id="my_spark_conn",
        application="/usr/local/airflow/include/scripts/silver.py",
        verbose=True,
        conf={
            "spark.submit.deployMode": "client"
        }
    )
    
    # ----- Gold Layer Job-----
    gold_job = SparkSubmitOperator(
    task_id="gold_job",
    application="/usr/local/airflow/include/scripts/gold.py",
    conn_id="my_spark_conn",
    conf={  
        "spark.submit.deployMode": "client",
        "spark.jars": "/usr/local/airflow/jars/postgresql-42.7.3.jar",
        "spark.driver.extraClassPath": "/usr/local/airflow/jars/postgresql-42.7.3.jar",
        "spark.executor.extraClassPath": "/usr/local/airflow/jars/postgresql-42.7.3.jar"
    },
    verbose=True
)

    bronze_job >> silver_job >> gold_job

ETL_dag()