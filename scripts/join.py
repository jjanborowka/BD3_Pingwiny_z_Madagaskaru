import logging
from pyspark.sql import SparkSession
import  pyspark.sql.functions as  F
import sys
import subprocess

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
    # MINUTES_DELAY = 15
    # start Spark session
    spark = SparkSession.builder.appName("SparkDemo").enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Starting spark application")
    
    logger.info("Reading Files")
    weather = spark.read.format('csv').options(header='true', inferschema='true').load('hdfs://localhost:8020/user/ficp/projekt/weather/weather.csv')
    buses = spark.read.format('csv').options(header='true', inferschema='true').load('hdfs://localhost:8020/user/ficp/projekt/ztm/ztm.csv')

    logger.info('Creating keys')

    weather = weather.withColumn("key", F.concat(F.col("last_updated").substr(0,10), F.lit("-"), F.col("last_updated").substr(12,2)))
    buses = buses.withColumn("key", F.concat(F.col("Time").substr(0,10), F.lit("-"), F.col("Time").substr(12,2)))

    # logger.info('Filtering by time')

    # filter buses to be from last 15 minutes
    # current_time  = str(datetime.now() - timedelta(minutes = MINUTES_DELAY)).split('.')[0]
    # buses = buses.withColumn('Time',F.to_timestamp(F.col('Time'), 'yyyy-MM-dd HH:mm:ss').alias('Time'))
    # buses = buses.filter(F.col('Time') > F.to_timestamp(F.lit(current_time)))
    
    logger.info('Writing to HDFS')

    #join data
    df = buses.join(weather, ['key'], "left")
    df.write.mode("append").format("parquet").partitionBy("key").saveAsTable("masterdf")
    
    # usun csv z autobusami
    subprocess.call(["hdfs", "dfs", "-rm", "-f", '/user/ficp/projekt/ztm/ztm.csv'])

    logger.info("Ending spark application")
    spark.stop()
    return None
    
# Starting point for PySpark
if __name__ == '__main__':
    main()
    sys.exit()
