import pyspark
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import sin, cos, asin, sqrt, col, pow, lit, row_number



def get_city_dict(city_dict_path: str, spark: SparkSession)-> pyspark.sql.DataFrame:
    # fetch cities dictionary and turn it into dataframe
    city_df = spark.read.csv(city_dict_path, sep=';', header=True)
    
    # rename columns
    cities = city_df\
             .withColumnRenamed("id","city_id")\
             .withColumnRenamed("lat","city_lat")\
             .withColumnRenamed("lng", "city_lon")\
             .withColumnRenamed("timezone","city_timezone")
    
    return cities


# preparation of partial events dataset 
def get_sampled_events(events_input_path:str, sample_fraction: float, spark: SparkSession) -> pyspark.sql.DataFrame:

    # prepare sample Dataset due to inital Dataset has more than 46M of records.
    if sample_fraction > 1:
        sample_fraction = 1

    events_sample = spark.read.parquet(events_input_path)\
                 .sample(sample_fraction)\
                 .withColumn('event_id', F.monotonically_increasing_id())

    return events_sample

# sphere 2 points distance calculator template
def get_sphere_points_distance():
    
    distance = 2 * F.lit(6371) * F.asin( 
                                        F.sqrt(
                                            F.pow(F.sin((F.radians(F.col("lat")) - F.radians(F.col("city_lat"))) / 2), 2) +
                                            F.cos(F.radians(F.col("lat"))) * F.cos(F.radians(F.col("city_lat"))) *
                                            F.pow(F.sin((F.radians(F.col("lon")) - F.radians(F.col("city_lon"))) / 2), 2)
                                            )
                                    )

    return distance
