import logging
from pyspark.sql import SparkSession
import  pyspark.sql.functions as  F
from pyspark.sql.types import IntegerType,DoubleType,DateType
#import pandas as pd
from pyspark.sql.functions import acos, cos, sin, lit, toRadians
from pyspark.sql.window import Window
from pyspark.sql.functions import lag
from datetime import datetime, timedelta
import sys
import subprocess
from datetime import datetime, timedelta


from datetime import datetime, timedelta
# Logging configuration
formatter = logging.Formatter('[%(asctime)s] %(levelname)s @ line %(lineno)d: %(message)s')
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
handler.setFormatter(formatter)
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(handler)

def main():
    # start Spark session
    spark = SparkSession.builder.appName("SparkDemo").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Starting spark application")

    # Read and filter to last whole hour
    df = spark.sql('SELECT * FROM masterdf')
    df = df.drop('Condition')
    df = df.withColumn('Time',F.to_timestamp(F.col('Time'), 'yyyy-MM-dd HH:mm:ss').alias('Time'))
    df = df.dropDuplicates(['Time','VehicleNumber'])
    df = df.withColumn('last_updated_epoch',F.from_unixtime(F.col('last_updated_epoch').cast('string')))

    time_limit = datetime.now()
    time_limit = time_limit.replace(minute=0, second=0, microsecond=0)
    df = df.filter(F.col('Time') < F.to_timestamp(F.lit(time_limit)))

    # Speed and distance calculation
    def dist(long_x, lat_x, long_y, lat_y):
        return acos(
            sin(toRadians(lat_x)) * sin(toRadians(lat_y)) +
            cos(toRadians(lat_x)) * cos(toRadians(lat_y)) *
                cos(toRadians(long_x) - toRadians(long_y))
        ) * lit(6371000.0)

    w = Window().partitionBy("VehicleNumber").orderBy("time")

    df = df.withColumn("dist", dist(
            "Lon", "Lat",
            F.lag("Lon", 1).over(w), F.lag("Lat", 1).over(w)
        ).alias("dist"))
    df = df.withColumn('timedelta',F.col('time').cast('long') - F.lag('time',1).over(w).cast('long'))

    df = df.withColumn('speed',3.6*(F.col('dist')/ F.col('timedelta')))
    df = df.dropna()

    # 1. Number of vehicles and lines
    df.select('VehicleNumber').distinct().groupBy().count()\
        .coalesce(1).write.mode("overwrite").json('hdfs://localhost:8020/user/ficp/projekt/analysis/number_vehicles_lines.json')

    # 2. Number of records per hour
    df.withColumn('hour',F.hour('Time')).groupBy('hour').count().orderBy(F.asc('hour'))\
        .coalesce(1).write.mode("overwrite").json('hdfs://localhost:8020/user/ficp/projekt/analysis/number_of_records.json')

    # 3. Number of active buses per hour
    df.withColumn('hour',F.hour('Time')).groupBy('hour').agg(F.countDistinct('VehicleNumber')).orderBy(F.asc('hour'))\
         .coalesce(1).write.mode("overwrite").json('hdfs://localhost:8020/user/ficp/projekt/analysis/number_of_buses_per_hour.json')

    # 4. Number of buses records per weather update
    df.groupBy('last_updated_epoch').count().orderBy('count')\
        .coalesce(1).write.mode("overwrite").json('hdfs://localhost:8020/user/ficp/projekt/analysis/buses_per_weather_update.json')

    # 5. Number of buses per line
    df.select(['Lines','VehicleNumber']).groupBy('Lines').agg(F.countDistinct('VehicleNumber')).orderBy(F.desc('count(VehicleNumber)'))\
        .coalesce(1).write.mode("overwrite").json('hdfs://localhost:8020/user/ficp/projekt/analysis/number_of_buses_per_line.json')

    # 6. Bus route plot
    # Reduce number of stored data
    d = datetime.now()
    from_d = d-timedelta(hours=23)
    for b in [7253, 15617, 80635, 7739, 4211]:
        df.filter(F.col('VehicleNumber') == b).filter(F.col('Time')>from_d)\
            .coalesce(1).write.mode("overwrite").json(f'hdfs://localhost:8020/user/ficp/projekt/analysis/bus_{b}_route.json')

    # 7. Fastest buses
    df.groupBy('VehicleNumber').agg(F.avg('speed')).orderBy(F.desc('avg(speed)'))\
        .coalesce(1).write.mode("overwrite").json('hdfs://localhost:8020/user/ficp/projekt/analysis/fastest_buses.json')

    # 8. Fastest lines
    df.groupBy('Lines').agg(F.avg('speed')).orderBy(F.desc('avg(speed)'))\
        .coalesce(1).write.mode("overwrite").json('hdfs://localhost:8020/user/ficp/projekt/analysis/fastest_lines.json')

    # 9. Precip
    df.withColumn('hour',F.hour('Time')).dropna().groupBy('hour').agg(F.avg('speed'),F.avg('precip_mm')).orderBy(F.desc('hour'))\
        .coalesce(1).write.mode("overwrite").json('hdfs://localhost:8020/user/ficp/projekt/analysis/speed_precip.json')

    # 10. Temp
    df.withColumn('hour',F.hour('Time')).dropna().groupBy('hour').agg(F.avg('speed'),F.avg('temp_c')).orderBy(F.desc('hour'))\
        .coalesce(1).write.mode("overwrite").json('hdfs://localhost:8020/user/ficp/projekt/analysis/speed_temp.json')

    # 11. Heatmap
    df = df.withColumn('agg_Lat',F.round(F.col('Lat'),4))
    df = df.withColumn('agg_Lon',F.round(F.col('Lon'),4))
    df.groupBy(['agg_Lat','agg_Lon','key']).count().orderBy(F.col('key'))\
        .coalesce(1).write.mode("overwrite").json('hdfs://localhost:8020/user/ficp/projekt/analysis/heatmap.json')


# Starting point for PySpark
if __name__ == '__main__':
    main()
    sys.exit()
