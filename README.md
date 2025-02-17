

# Spint 7 project

### Description
This repository is intended for source code of Sprint 7 project.  

***Technologies used in implementation:***
1. HDFS
2. YARN
3. Spark
4. Python (pyspark)
5. Jupyter Notebook
6. Airflow

### Repository structure
Inside `src` next folders exist:
- `/src/dags` - DAG for datalake population.
- `/src/scripts` - spark jobs to calculate data marts.

### Datalake structure
1. Staging with raw data - /user/master/data/geo/events; parquet
2. ODS with sampled data - /user/nabordotby/data/sample/, subfolders mart_1, mart_2, mart_3; parquet
3. Sandbox for analytics /user/nabordotby/data/analytics/ , subfolders mart_1, mart_2, mart_3; parquet
Daily data refresh


