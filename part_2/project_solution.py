from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 8, 4, 0, 0),
}

connection_name = "goit_mysql_db_oleksiik"

with DAG(
        'ok_fp_multi_hop_dag',
        default_args=default_args,
        schedule_interval=None,
        catchup=False,
        tags=["oleksiik_fp"]
) as dag:
    landing_to_bronze_task = SparkSubmitOperator(
        application='landing_to_bronze.py',
        task_id='landing_to_bronze',
        conn_id='spark-default',
        verbose=1,
        dag=dag,
    )

    bronze_to_silver_task = SparkSubmitOperator(
        application='bronze_to_silver.py',
        task_id='bronze_to_silver',
        conn_id='spark-default',
        verbose=1,
        dag=dag,
    )

    silver_to_gold_task = SparkSubmitOperator(
        application='silver_to_gold.py',
        task_id='silver_to_gold',
        conn_id='spark-default',
        verbose=1,
        dag=dag,
    )

    landing_to_bronze_task >> bronze_to_silver_task >> silver_to_gold_task
