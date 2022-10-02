import sys
import logging
import pyspark.sql.functions as F 
import datetime as dt
from pyspark.sql.window import Window 

from pyspark.sql import SparkSession
    

def user_zone_report():
    try:
        date = sys.argv[1]
        dir_name_from = sys.argv[2]
        dir_name_to = sys.argv[3]

        spark = SparkSession\
            .builder.appName(f"UserZoneReport")\
            .config("spark.dynamicAllocation.enabled", "true")\
            .getOrCreate()

        logging.info("SparkSession was created successfully")
        
        data_geo = spark.read\
            .parquet('hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/ilyasmolin/data/geo/city_coordinates')
        
        data_messages = spark.read\
            .parquet(dir_name_from)\
            .where(f"event_type='message' and nearest_city is not null and date <= '{date}'")\
        
        logging.info("data_messages was loaded successfully")
        
        ordered_user_cities = data_messages\
            .select(F.col('event.message_from').alias('user_id'), F.col('nearest_city').alias('act_city'), 'event.message_ts')\
            .withColumn(
                "rank", 
                F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("message_ts")))
            )\
        
        logging.info("ordered_user_cities was loaded successfully")
        
        act_city = ordered_user_cities\
            .where("rank = 1")\
            .select('user_id', 'act_city')
        
        logging.info("act_city was loaded successfully")
        
        last_message_ts = ordered_user_cities.select(F.max('message_ts')).collect()[0][0]
        
        shift_data = ordered_user_cities\
            .withColumn(
                "lag_act_city_-1", 
                F.lag('act_city', -1, 'start').over(Window.partitionBy("user_id").orderBy(F.desc("message_ts")))
            )\
            .withColumn(
                "lag_message_ts_+1", 
                F.lag('message_ts', 1, last_message_ts).over(Window.partitionBy("user_id",).orderBy(F.desc("message_ts")))
            )\
        
        logging.info("shift_data was loaded successfully")

        home_city = shift_data\
            .withColumn('duration_days', (F.to_timestamp("lag_message_ts_+1").cast("long") - F.to_timestamp('message_ts').cast("long")) / (24*3600))\
            .select('user_id', 'act_city', 'message_ts', 'duration_days')\
            .groupBy('user_id', 'act_city')\
            .agg(F.sum("duration_days").alias('duration_days'), F.max("message_ts").alias('last_message_ts'))\
            .where('duration_days > 27')\
            .withColumn(
                "rank_home_city", 
                F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("last_message_ts")))
            )\
            .where('rank_home_city = 1')\
            .join(data_geo.select(F.col('city').alias('act_city'), F.col('timezone').alias('timezone_copy')), on='act_city', how='left')\
            .withColumn('timezone', F.concat(F.lit('Australia/'), F.col('timezone_copy')))\
            .withColumn('local_time', F.from_utc_timestamp(F.col("last_message_ts"), F.col('timezone')))\
            .select('user_id', F.col('act_city').alias('home_city'), 'local_time')\
            
        logging.info("home_city was loaded successfully")
            
        travel_data = shift_data\
            .withColumn('eq_act_city', F.col('act_city') == F.col('lag_act_city_-1'))\
            .where('eq_act_city is false')\
            .select('user_id', 'act_city')\
            .groupBy('user_id')\
            .agg(F.count("act_city").alias('travel_count'), F.collect_list("act_city").alias('travel_array'))\
        
        logging.info("travel_data was loaded successfully")
        
        report = act_city\
            .join(home_city, on='user_id', how='fullouter')\
            .join(travel_data, on='user_id', how='fullouter')\
        
        logging.info("report was loaded successfully")

        report.write\
            .mode("overwrite")\
            .parquet(dir_name_to)
        
        logging.info(f"{dir_name_to} was written")
        
    except:
        logging.exception("An exception was thrown!")
    finally:
        spark.stop()


if __name__ == '__main__':
    user_zone_report()
