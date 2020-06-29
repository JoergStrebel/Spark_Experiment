#!/usr/bin/python
# -*- coding: utf-8 -*-
from __future__ import print_function

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql import Window, WindowSpec

def parquet_datasource(spark, file, logger):
    df = spark.read.parquet(file)
    logger.info(df.rdd.getNumPartitions())
    return df

now = datetime.now() # current date and time
FILE_IN = '../testdata_out20200628155222/'
FILE_OUT = '../dedupdata_out'+ now.strftime("%Y%m%d%H%M%S")


if __name__ == "__main__":

    myconf = SparkConf() \
        .setAppName("Spark Data Test") \
        .setSparkHome("/home/jstrebel/devel/Spark_Experiment/pyspark-test/bin/")\
        .set("spark.local.dir", "/home/jstrebel/devel/Spark_Experiment/tmp")
    myconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    sc = SparkContext(conf=myconf)
    sc.setLogLevel(logLevel="INFO")

    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)

    spark = SparkSession(sc) \
        .builder \
        .config('spark.sql.parquet.compression.codec', 'snappy') \
        .config('spark.sql.parquet.filterPushdown','true') \
        .config('spark.dynamicAllocation.enabled','false') \
        .config('spark.shuffle.service.enabled','false') \
        .getOrCreate()

    df = parquet_datasource(spark, FILE_IN, log)

    df = df.drop(df.sortts).drop(df.timedesc)

    df.show(10)
    log.info(f'Number of partitions: {df.rdd.getNumPartitions()}')

    # Variant 1:
    #df = df.groupBy(['idxnr', 'countnr']).agg({'rndsortts':'max'})
    dfagg = df.groupBy([df.idxnr, df.countnr]).agg(F.max(df.rndsortts))
    dfagg = dfagg.select(dfagg.idxnr, dfagg.countnr, dfagg['max(rndsortts)'].alias('maxrndsortts'))
    dfagg.cache()

    cond = [df.idxnr == dfagg.idxnr, df.countnr == dfagg.countnr, df.rndsortts == dfagg.maxrndsortts]
    dfdedup = df.join(dfagg, cond, 'inner').select(dfagg.idxnr, dfagg.countnr, dfagg.maxrndsortts)

    #Variant 2:
    #windowSpec = Window.partitionBy([df.idxnr, df.countnr])
    #df.rdd.getNumPartitions()
    #windowSpec = windowSpec.withColumn('max_date', F.max(df.rndsortts).over(windowSpec))\
    #    .filter((df.rndsortts == df.max_date)).drop('max_date')


    dfdedup = dfdedup.coalesce(10)
    dfdedup.write.parquet(FILE_OUT, mode='overwrite')

    dfdedup.show(10)

    sc.stop()


