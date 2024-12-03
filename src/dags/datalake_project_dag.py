from datetime import datetime
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'


events_source = '/user/master/data/geo/events'
city_dict_source = '/user/nabordotby/data/city_dict'
output_path = '/user/nabordotby/analytics/mart_1'

default_args = {
                'owner': 'airflow',
                'start_date':datetime(2020, 1, 1),
        }

dag = DAG(
                dag_id = "Project",
                default_args=default_args,
                schedule_interval="@daily",
            )

# define task for step2
calculate_user_mart = SparkSubmitOperator(
                        task_id='calculate_mart_for_step_2',
                        dag=dag,
                        application ='/lessons/scripts/step_2_mart.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [events_source, city_dict_source, output_path],
                        conf={
                                "spark.driver.maxResultSize": "20g"
                            },
                        executor_cores = 2,
                        executor_memory = '2g'
            )