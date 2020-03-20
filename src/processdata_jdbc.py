#!/usr/bin/python
# -*- coding: utf-8 -*-

from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime

def jdbc_dataset_example(spark, log, file_out):

    jdbcDF = spark.read\
        .format("jdbc")\
        .option("url", "jdbc:postgresql://localhost:5432/pysparkdb")\
        .option("dbtable", "public.testdataidx")\
        .option("user", "jstrebel")\
        .option("password", "")\
        .option("driver", "org.postgresql.Driver")\
        .option("fetchsize", 1000000)\
        .option("pushDownPredicate", True)\
        .option("numPartitions", 2)\
        .option("partitionColumn","idxnr")\
        .option("lowerBound",0)\
        .option("upperBound", 1000000)\
        .load()

    #jdbcDF = jdbcDF.filter(jdbcDF[0] < 10000)
    jdbcDF.explain(True)
    #jdbcDF=jdbcDF.sort("idxnr", ascending=True)
    #print(jdbcDF.rdd.toDebugString())
    #print(df.rdd.toDebugString())  #only sensible for pyspark-shell
    #log.info(df.rdd.getNumPartitions())
    #jdbcDF.write.csv(file_out, mode='overwrite')
    jdbcDF.write.parquet(file_out, mode='overwrite')



now = datetime.now() # current date and time
FILE_OUT = '../testdata_out'+ now.strftime("%Y%m%d%H%M%S")

if __name__ == "__main__":

    sc = SparkContext(appName="Spark Data Test")
    sc.setLogLevel(logLevel="INFO")

    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)

    spark = SparkSession(sc) \
        .builder \
        .config('spark.sql.parquet.compression.codec', 'snappy') \
        .config('spark.sql.parquet.filterPushdown','true') \
        .config('spark.dynamicAllocation.enabled','true') \
        .config('spark.shuffle.service.enabled','true') \
    .getOrCreate()

    # basic_rdd_example(sc, FILE_IN, FILE_OUT, log)
    jdbc_dataset_example(spark, log, FILE_OUT)

    sc.stop()
