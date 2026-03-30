from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    "owner": "data_engineer",
    "start_date": datetime(2025, 1, 1)
}

with DAG(
    dag_id="gcp_medallion_pipeline",
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["spark","gcp","medallion"]
) as dag:

    bronze_task = BashOperator(
        task_id="bronze_layer",
        bash_command="""
/opt/spark/bin/spark-submit \
--jars /opt/airflow/jars/gcs-connector.jar \
--conf spark.hadoop.google.cloud.auth.service.account.enable=true \
--conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/airflow/config/gcp_credentials.json \
--conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
--conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
/opt/airflow/spark_jobs/bronze_layer.py
"""
    )

    silver_task = BashOperator(
        task_id="silver_layer",
        bash_command="""
/opt/spark/bin/spark-submit \
--jars /opt/airflow/jars/gcs-connector.jar \
--conf spark.hadoop.google.cloud.auth.service.account.enable=true \
--conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/airflow/config/gcp_credentials.json \
--conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
--conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
/opt/airflow/spark_jobs/silver_layer.py
"""
    )

    gold_task = BashOperator(
        task_id="gold_layer",
        bash_command="""
/opt/spark/bin/spark-submit \
--jars /opt/airflow/jars/gcs-connector.jar,/opt/airflow/jars/spark-bigquery.jar \
--conf spark.hadoop.google.cloud.auth.service.account.enable=true \
--conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/airflow/config/gcp_credentials.json \
--conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
--conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
/opt/airflow/spark_jobs/gold_layer.py
"""
    )
#     gold_task = BashOperator(
#     task_id="gold_layer",
#     bash_command="""
# /opt/spark/bin/spark-submit \
# --jars /opt/airflow/jars/gcs-connector.jar,/opt/airflow/jars/spark-bigquery.jar \
# --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
# --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/opt/airflow/config/gcp_credentials.json \
# /opt/airflow/spark_jobs/gold_layer.py
# """
# )
    bronze_task >> silver_task >> gold_task