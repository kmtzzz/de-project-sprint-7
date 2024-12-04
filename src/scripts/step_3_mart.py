import os
import findspark

findspark.init()
findspark.find()

from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.functions import sin, cos, asin, sqrt, col, pow, lit, row_number, when

import helper

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

spark = SparkSession.builder \
                    .master("yarn") \
                    .appName("project_s7_step_3") \
                    .getOrCreate()

# path for ODS 
sample_source = '/user/nabordotby/data/sample/mart_2'


def calculate_mart(events_input_path: str, city_dict: str, spark: SparkSession) -> pyspark.sql.DataFrame:
    
    # read city dictionary
    cities = get_city_dict(city_dict, spark).persist()
    # fetch sampled events with populated coordinates
    events = get_sampled_events('/user/master/data/geo/events', 0.03, spark)\
             .where(("lat is not null and lon is not null"))
    
    # and save to disk
    events.write.mode("overwrite").parquet(f'{sample_source}')

    # fetch dataset for further processing
    sampled_events = spark.read.parquet(sample_source)

    # find for each user message(event_id) closest city, define week and month of event
    distance_window = Window().partitionBy("event_id").orderBy(F.col("distance").asc())

    event_map_to_city = (sampled_events.crossJoin(cities)
                            .withColumn("distance", get_sphere_points_distance())
                            .withColumn("rn", F.row_number().over(distance_window))
                            .filter(F.col("rn")==1)
                            .withColumn("week", F.trunc(F.col("date"),"week"))
                            .withColumn("month", F.trunc(F.col("date"),"month")) 
                            .drop("rn","lat","lon","city_lat","city_lon","distance")
                            .persist()
                        )

    # build dataset
    message_window = Window().partitionBy("event.message_from", "city").orderBy(F.col('date'))
    week_window = Window().partitionBy("week", "city")
    month_window = Window().partitionBy("month", "city")

    result = (event_map_to_city
                   .withColumn("rn",F.row_number().over(message_window))
                   .withColumn("week_message",F.sum(F.when(event_map_to_city.event_type == "message",1).otherwise(0)).over(week_window))
                   .withColumn("week_reaction",F.sum(F.when(event_map_to_city.event_type == "reaction",1).otherwise(0)).over(week_window))
                   .withColumn("week_subscription",F.sum(F.when(event_map_to_city.event_type == "subscription",1).otherwise(0)).over(week_window))
                   .withColumn("week_user",F.sum(F.when(F.col("rn") == 1,1).otherwise(0)).over(week_window))
                   .withColumn("month_message",F.sum(F.when(event_map_to_city.event_type == "message",1).otherwise(0)).over(month_window))
                   .withColumn("month_reaction",F.sum(F.when(event_map_to_city.event_type == "reaction",1).otherwise(0)).over(month_window))
                   .withColumn("month_subscription",F.sum(F.when(event_map_to_city.event_type == "subscription",1).otherwise(0)).over(month_window))
                   .withColumn("month_user",F.sum(F.when(F.col("rn") == 1,1).otherwise(0)).over(month_window))
                   .select(
                       "month", 
                       "week", 
                       F.col("city_id").alias("zone_id"),
                       "week_message",
                       "week_reaction",
                       "week_subscription",
                       "week_user",
                       "month_message",
                       "month_reaction",
                       "month_subscription",
                       "month_user")
                       .distinct()
                   .persist()
     )
    
    return result


def main():
    spark = SparkSession \
        .builder \
        .master("yarn") \
        .config("spark.driver.cores", "2") \
        .config("spark.driver.memory", "2g") \
        .appName("project_s7_step_2") \
        .getOrCreate()

    events_input_path = sys.argv[1]
    city_dict = sys.argv[2]
    output_path = sys.argv[3]

    mart_df = calculate_mart(events_input_path, city_dict, spark)
        
    mart_df.write.mode("overwrite").parquet(f'{output_path}')

if __name__ == '__main__':
    main()