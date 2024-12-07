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
output_path = '/user/nabordotby/data/analytics'

default_args = {
                'owner': 'airflow',
                'start_date':datetime(2024, 12, 7),
        }

dag = DAG(
                dag_id = "s7_project_datalake",
                default_args=default_args,
                schedule_interval="@daily",
            )

# define task for step 2
calculate_step_2_mart = SparkSubmitOperator(
                        task_id='calculate_mart_for_step_2',
                        dag=dag,
                        application ='/lessons/scripts/step_2_mart.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [events_source, city_dict_source, f'{output_path}/mart_1'],
                        conf={
                                "spark.driver.maxResultSize": "20g"
                            },
                        executor_cores = 2,
                        executor_memory = '2g'
            )


# define task for step 3
calculate_step_3_mart = SparkSubmitOperator(
                        task_id='calculate_mart_for_step_3',
                        dag=dag,
                        application ='/lessons/scripts/step_3_mart.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [events_source, city_dict_source, f'{output_path}/mart_2'],
                        conf={
                                "spark.driver.maxResultSize": "20g"
                            },
                        executor_cores = 2,
                        executor_memory = '2g'
            )

# define task for step 4
calculate_step_4_mart = SparkSubmitOperator(
                        task_id='calculate_mart_for_step_4',
                        dag=dag,
                        application ='/lessons/scripts/step_4_mart.py' ,
                        conn_id= 'yarn_spark',
                        application_args = [events_source, city_dict_source, f'{output_path}/mart_3'],
                        conf={
                                "spark.driver.maxResultSize": "20g"
                            },
                        executor_cores = 2,
                        executor_memory = '2g'
            )

calculate_step_2_mart >> calculate_step_3_mart >> calculate_step_4_mart