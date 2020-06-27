#!/usr/bin/python
# -*- coding: utf-8 -*-

from pyspark import SparkContext, SparkConf
from datetime import datetime
from pyspark.streaming import StreamingContext


now = datetime.now() # current date and time
FILE_OUT = '../testdata_out'+ now.strftime("%Y%m%d%H%M%S")

if __name__ == "__main__":
    myconf = SparkConf()\
        .setAppName("Spark Data Test")\
        .setSparkHome("/home/jstrebel/devel/Spark_Experiment/pyspark-test/bin/")
    myconf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    sc = SparkContext(conf=myconf)
    sc.setLogLevel(logLevel="INFO")
    ssc = StreamingContext(sc, 1)  # 1 second of data in one microbatch

    lines = ssc.socketTextStream("localhost", 9999)
    lines.pprint()
    lines.saveAsTextFiles("testdata_out")

    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)

    ssc.start()             # Start the computation
    ssc.awaitTermination()  # Wait for the computation to terminate
    sc.stop()
