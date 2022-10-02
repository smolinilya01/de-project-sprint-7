import sys
import logging
import pyspark.sql.functions as F 
import datetime as dt
from pyspark.sql.window import Window 

from pyspark.sql import SparkSession
 
    
def dist_count(lat1, lat2, lon1, lon2):
    part1 = F.pow(F.sin((lat2 - lat1) / 2), 2)
    part2 = F.cos(lat1)
    part3 = F.cos(lat2)
    part4 = F.pow(F.sin((lon2 - lon1) / 2), 2)
    
    dist = 2 * 6371 * F.asin( F.sqrt(part1 + (part2 * part3 * part4)) )
    
    return dist


def recommendation_zone_report():
    try:
        date = sys.argv[1]
        dir_name_from = sys.argv[2]
        dir_name_to = sys.argv[3]

        spark = SparkSession\
            .builder.appName(f"RecomendationZoneReport")\
            .config("spark.dynamicAllocation.enabled", "true")\
            .getOrCreate()

        logging.info("SparkSession was created successfully")
        
        data_geo = spark.read\
            .parquet('hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/ilyasmolin/data/geo/city_coordinates')
        
        data_messages = spark.read\
            .parquet(dir_name_from)\
            .where(f"event_type='message' and nearest_city is not null and date <= '{date}'")\
        
        
        logging.info("data_messages was loaded successfully")
        
        
        users = data_messages\
            .select(F.col('event.message_from'))\
            .distinct()\
        
        cross_users = users\
            .crossJoin(users.select(F.col('message_from').alias('message_to')))\
            .join(data_messages.select('event.message_from', 'event.message_to').distinct(), on=['message_from', 'message_to'], how='leftanti')\
            .where('message_from != message_to')\
            
        users_pairs = cross_users\
            .withColumn("users_pairs", F.array_sort(F.array("message_from", 'message_to')))\
            .select('users_pairs')\
            .distinct()\
            .orderBy('users_pairs')\
            .select(F.col('users_pairs')[0].alias('user_left'), F.col('users_pairs')[1].alias('user_right'))
        
        logging.info("users_pairs was loaded successfully")
        
        
        last_messages = data_messages\
            .select('event.message_from', 'lat', 'lon', 'event.message_ts', 'nearest_city')\
            .withColumn(
                "rank", 
                F.row_number().over(Window.partitionBy("message_from").orderBy(F.desc("message_ts")))
            )\
            .where('rank = 1')
        
        logging.info("last_messages was loaded successfully")

        
        data_subscription = spark.read\
            .parquet(dir_name_from)\
            .where("event_type='subscription'")\
            .select('event.user', 'event.subscription_channel')\
            .distinct()\
            .groupBy('user')\
            .agg(F.collect_list("subscription_channel").alias('subscription_channel'))\
        
        logging.info("data_subscription was loaded successfully")

        
        report = users_pairs\
            .join(
                last_messages.select(F.col('message_from').alias('user_left'), F.col('lat').alias('user_left_lat'), F.col('lon').alias('user_left_lon'), F.col('nearest_city').alias('zone_id')), 
                on='user_left',
                how='left'
            )\
            .join(
                last_messages.select(F.col('message_from').alias('user_right'), F.col('lat').alias('user_right_lat'), F.col('lon').alias('user_right_lon')), 
                on='user_right',
                how='left'
            )\
            .withColumn(
                'distance', 
                dist_count(
                    lat1=F.col('user_left_lat'),
                    lat2=F.col('user_right_lat'),
                    lon1=F.col('user_left_lon'),
                    lon2=F.col('user_right_lon')
                ) 
            )\
            .where("distance <= 1000")\
            .select('user_left', 'user_right', 'zone_id')\
            .join(
                data_subscription.select(F.col('user').alias('user_left'), F.col('subscription_channel').alias('user_left_subscription_channel')), 
                on='user_left',
                how='left'
            )\
            .join(
                data_subscription.select(F.col('user').alias('user_right'), F.col('subscription_channel').alias('user_right_subscription_channel')), 
                on='user_right',
                how='left'
            )\
            .withColumn('channels_intersect', F.array_intersect('user_left_subscription_channel', 'user_right_subscription_channel'))\
            .filter(F.size(F.col('channels_intersect')) >= 1)\
            .withColumn("processed_dttm", F.current_date())\
            .join(data_geo.select(F.col('city').alias('zone_id'), F.col('timezone')), on='zone_id', how='left')\
            .withColumn('timezone_city', F.concat(F.lit('Australia/'), F.col('timezone')))\
            .withColumn('local_time', F.from_utc_timestamp(F.current_timestamp(), F.col('timezone_city')))\
            .select('user_left', 'user_right', 'processed_dttm', 'zone_id', 'local_time')\
        
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
    recommendation_zone_report()
