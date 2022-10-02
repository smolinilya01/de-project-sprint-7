import sys
import logging
import pyspark.sql.functions as F 
import datetime as dt

from pyspark.sql import SparkSession
    

def week_zone_report():
    try:
        date = sys.argv[1]
        dir_name_from = sys.argv[2]
        dir_name_to = sys.argv[3]

        spark = SparkSession\
            .builder.appName(f"WeekZoneReport-{date}")\
            .config("spark.dynamicAllocation.enabled", "true")\
            .getOrCreate()
    
        cur_date = dt.datetime.strptime(date, '%Y-%m-%d')
        week_start = cur_date - dt.timedelta(days=cur_date.weekday())
        week_end = week_start + dt.timedelta(days=6)
        month_start = cur_date - dt.timedelta(days=cur_date.day-1)


        logging.info("SparkSession was created successfully")
        
        data_events = spark.read\
            .parquet(dir_name_from)\
            .where(f"date>='{month_start.strftime('%Y-%m-%d')}' and date < '{date}'")\
            .withColumn("month", F.month(F.col('date')))\
            .withColumn("week", F.weekofyear(F.col('date')))\
            .where(F.col("nearest_city").isNotNull())\
        
        if data_events.rdd.isEmpty():
            return None
        
        logging.info("data_events was loaded successfully")
        
        data_events_week = data_events\
            .groupBy("month", "week", 'nearest_city')\
            .pivot('event_type')\
            .count()\
            .select(
                "month", 
                "week", 
                'nearest_city', 
                F.col('message').alias('week_message'), 
                F.col('reaction').alias('week_reaction'),
                F.col('subscription').alias('week_subscription')
            )
        
        data_events_month = data_events\
            .groupBy("month", 'nearest_city')\
            .pivot('event_type')\
            .count()\
            .select(
                "month", 
                'nearest_city', 
                F.col('message').alias('month_message'), 
                F.col('reaction').alias('month_reaction'),
                F.col('subscription').alias('month_subscription')
            )
        
        report = data_events_week.join(data_events_month, on=["month", 'nearest_city'], how='left')
        logging.info("report was loaded successfully")
        
        report.write\
            .mode("overwrite")\
            .parquet(dir_name_to + f"/month={date.split('-')[1]}")
        
        logging.info(f"{dir_name_to}/month={date.split('-')[1]} was written")
        
    except:
        logging.exception("An exception was thrown!")
    finally:
        spark.stop()


if __name__ == '__main__':
    week_zone_report()
