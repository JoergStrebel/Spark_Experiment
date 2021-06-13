#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
Module for testing the error propagation in statistical function in Apache Spark for floating-point numbers.
"""

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession, DataFrame
from datetime import datetime
import random
from pyspark.sql.types import *
import numpy as np
import math

VOLUME = 500000  #10000000 creates a file of ca. 417 MB
NUMRANGE = 1000000

def rnd_number_sequence(nint):
    """
    Generator for test data
    my test data has two columns : float, float
    """
    num = 0
    while num<nint:
        yield (random.randint(0,NUMRANGE)/NUMRANGE, random.randint(0,NUMRANGE)/NUMRANGE)
        num += 1

def const_number_sequence(nint):
    """
    Generator for test data
    my test data has two columns : float, float
    """
    num = 0
    while num<nint:
        yield (0.1, 0.1)
        num += 1


#testdata = {'index': [x for x in range(VOLUME)], 'name': ['Zahl '+str(x) for x in range(VOLUME)]}
#pd.DataFrame(data=testdata).to_csv('/home/jstrebel/devel/pyspark-test/testdata.csv', index=False, header=False, quoting=csv.QUOTE_NONNUMERIC)

def process_df(df: DataFrame, log, msg:str):
    """
    Process the dataframe and calculate the error
    """

    #jdbcDF = jdbcDF.filter(jdbcDF[0] < 10000)
    df.explain(True)
    #jdbcDF=jdbcDF.sort("idxnr", ascending=True)
    #print(jdbcDF.rdd.toDebugString())
    df.printSchema()
    df.rdd.toDebugString()  #only sensible for pyspark-shell
    log.info(f"Partitons {df.rdd.getNumPartitions()}")
    #df.select("num1", "num2").summary().show()
    #df.select("num1", "num2").describe().show()
    erglist=df.groupBy().sum('num1', "num2").collect()
    print(f"Apache Spark {msg}: \t\t num1 {erglist[0][0]:.10f}, num2 {erglist[0][1]:.10f}")

#jdbcDF.write.csv(file_out, mode='overwrite')
    #jdbcDF.write.parquet(file_out, mode='overwrite')


if __name__ == "__main__":
    # init the iterator
    numgen = rnd_number_sequence(VOLUME)
    numlist = list(numgen)

    numgen1 = const_number_sequence(VOLUME)
    constlist = list(numgen1)

    # RND
    # the naive Python approach
    erglist=[sum(x) for x in zip(*numlist)]
    print(f"Naive Sum RND: \t\t num1 {erglist[0]:.10f}, num2 {erglist[1]:.10f}")

    # the corrected Python approach
    erglist=[math.fsum(x) for x in zip(*numlist)]
    print(f"FP Sum RND: \t\t num1 {erglist[0]:.10f}, num2 {erglist[1]:.10f}")

    # the numpy approach 64bit
    numarray= np.array(numlist,dtype=np.float64)
    erglist=np.sum(numarray, axis=0, dtype=np.float64)
    print(f"NP 64bit RND: \t\t num1 {erglist[0]:.10f}, num2 {erglist[1]:.10f}")

    # the numpy approach 32bit
    numarray= np.array(numlist,dtype=np.float32)
    erglist=np.sum(numarray, axis=0, dtype=np.float32)
    print(f"NP 32bit RND: \t\t num1 {erglist[0]:.10f}, num2 {erglist[1]:.10f}")

    # CONST
    # the naive Python approach
    erglist=[sum(x) for x in zip(*constlist)]
    print(f"Naive Sum CONST: \t\t num1 {erglist[0]:.10f}, num2 {erglist[1]:.10f}")

    # the corrected Python approach
    erglist=[math.fsum(x) for x in zip(*constlist)]
    print(f"FP Sum CONST: \t\t num1 {erglist[0]:.10f}, num2 {erglist[1]:.10f}")

    # the numpy approach 64bit
    numarray= np.array(constlist,dtype=np.float64)
    erglist=np.sum(numarray, axis=0, dtype=np.float64)
    print(f"NP 64bit CONST: \t\t num1 {erglist[0]:.10f}, num2 {erglist[1]:.10f}")

    # the numpy approach 32bit
    numarray= np.array(constlist,dtype=np.float32)
    erglist=np.sum(numarray, axis=0, dtype=np.float32)
    print(f"NP 32bit CONST: \t\t num1 {erglist[0]:.10f}, num2 {erglist[1]:.10f}")

    # the numpy approach long double
    numarray= np.array(constlist,dtype=np.longdouble)
    erglist=np.sum(numarray, axis=0, dtype=np.longdouble)
    print(f"NP longdouble CONST: \t\t num1 {erglist[0]:.10f}, num2 {erglist[1]:.10f}")

    # the PySpark approach

    myconf = SparkConf() \
        .setAppName("Spark Floating Point Test") \
        .setSparkHome("/mnt/home/jstrebel/devel/Spark_Experiment/pyspark-test/bin/")\
        .set("spark.local.dir", "./tmp")
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

    schema = StructType([
        StructField("num1", DoubleType(), True),
        StructField("num2", DoubleType(), True)])
    df=spark.createDataFrame(numlist, schema)

    schema1 = StructType([
        StructField("num1", FloatType(), True),
        StructField("num2", FloatType(), True)])
    df1=spark.createDataFrame(numlist, schema1)

    df2=spark.createDataFrame(constlist, schema)

    df3=spark.createDataFrame(constlist, schema1)

    # basic_rdd_example(sc, FILE_IN, FILE_OUT, log)
    process_df(df, log, "Double RND")
    process_df(df1, log, "Float RND")
    process_df(df2, log, "Double CONST")
    process_df(df3, log, "Float CONST")

    sc.stop()

