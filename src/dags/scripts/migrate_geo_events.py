import sys
import logging
import pyspark.sql.functions as F 

from pyspark.sql import SparkSession


def to_float(n):
    return float(n.replace(',', '.'))


def dist_count(lat1, lat2, lon1, lon2):
    part1 = F.pow(F.sin((lat2 - lat1) / 2), 2)
    part2 = F.cos(lat1)
    part3 = F.cos(lat2)
    part4 = F.pow(F.sin((lon2 - lon1) / 2), 2)
    
    dist = 2 * 6371 * F.asin( F.sqrt(part1 + (part2 * part3 * part4)) )
    
    return dist
    

def distance_enriched():
    try:
        date = sys.argv[1]
        dir_name_from = sys.argv[2]
        dir_name_to = sys.argv[3]
        
        spark = SparkSession\
            .builder.appName(f"MigrateGeoEvents-{date}")\
            .config("spark.dynamicAllocation.enabled", "true")\
            .getOrCreate()
        
        logging.info("SparkSession was created successfully")
        
        data_events = spark.read\
            .parquet(dir_name_from)\
            .where(f"date='{date}'")
        
        if data_events.rdd.isEmpty():
            return None
        
        logging.info("data_events was loaded successfully")
        
        data_geo = spark.read\
            .parquet('hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/ilyasmolin/data/geo/city_coordinates')
        
        logging.info("data_geo was loaded successfully")
        
        for geo_row in data_geo.collect():
            data_events = data_events.withColumn(
                geo_row['city'], 
                dist_count(
                    lat1=F.lit(to_float(geo_row['lat'])),
                    lat2=F.col('lat'),
                    lon1=F.lit(to_float(geo_row['lng'])),
                    lon2=F.col('lon')
                ) 
            )
        
        city_columns = [i['city'] for i in data_geo.collect()]
        
        cond = "F.when" + ".when".join(["(F.col('" + c + "') == F.col('dist_to_nearest_city'), F.lit('" + c + "'))" for c in city_columns])
        
        data_events = data_events\
            .withColumn("dist_to_nearest_city", F.least(*city_columns))\
            .withColumn("nearest_city", eval(cond))\
        
        data_events = data_events.drop(*city_columns)
        
        logging.info("data_events was enriched successfully")
        
        data_events.write\
            .mode("overwrite")\
            .partitionBy('event_type')\
            .parquet(dir_name_to + f"/date={date}")
        
        logging.info(f"{dir_name_to}/date={date} was written")
        
    except:
        logging.exception("An exception was thrown!")


if __name__ == '__main__':
    distance_enriched()