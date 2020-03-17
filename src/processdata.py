from __future__ import print_function
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import Row

def basic_datasource_example(spark, file, file2, logger):
    df = spark.read.load(file, format="csv", sep=",", header="false")
    df = df.filter(df[0] < 10000)
    df.explain(True)
    #print(df.rdd.toDebugString())  #only sensible for pyspark-shell
    #log.info(df.rdd.getNumPartitions())
    df.write.csv(file2, mode='overwrite')


FILE_IN = '../testdata_large.csv'
FILE_OUT = '../testdata_out'

if __name__ == "__main__":

    sc = SparkContext(appName="Spark Data Test")
    sc.setLogLevel(logLevel="INFO")

    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)

    spark = SparkSession(sc) \
        .builder \
        .getOrCreate()

    basic_datasource_example(spark, FILE_IN, FILE_OUT, log)

    spark.stop()
