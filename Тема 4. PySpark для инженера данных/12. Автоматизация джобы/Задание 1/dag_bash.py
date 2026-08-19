from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os


DAG_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. Формируем полный путь к вашему Spark-скрипту
SPARK_SCRIPT_PATH = os.path.join(DAG_DIR, "partition.py")

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

default_args = {
                                'owner': 'airflow',
                                'start_date': datetime(2026, 8, 19),
                                }

dag_spark = DAG(
                        dag_id = "sparkoperator_demo",
                        default_args=default_args,
                        schedule_interval=None,
                        )

# объявляем задачу с помощью SparkSubmitOperator
spark_submit_local = SparkSubmitOperator(
                        task_id='spark_submit_task',
                        dag=dag_spark,
                        application =SPARK_SCRIPT_PATH,
                        conn_id= 'yarn_spark',
                        application_args = [
                            "2022-05-31", "/user/master/data/events", "/user/username/analitics/events"
                        ],
                        conf={
                            "spark.driver.maxResultSize": "20g"
                        },
                        executor_cores = 2,
                        executor_memory = '2g'
                        )

spark_submit_local