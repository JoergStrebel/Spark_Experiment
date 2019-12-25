from __future__ import print_function
from pyspark import SparkContext, RDD
from datetime import datetime

def basic_rdd_example(sc, file, file2, logger):
    myrdd = sc.textFile(name=file, minPartitions=2, use_unicode=True)
    myrdd.filter(lambda x: int(x.split(',', maxsplit=1)[0]) < 10000).saveAsTextFile(path=file2)

now = datetime.now() # current date and time
FILE_IN = '/home/jstrebel/devel/pyspark-test/testdata_large.csv'
FILE_OUT = '/home/jstrebel/devel/pyspark-test/testdata_out'+ now.strftime("%Y%m%d%H%M%S")

if __name__ == "__main__":

    sc = SparkContext(appName="Spark Data Test")
    sc.setLogLevel(logLevel="INFO")

    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)

    basic_rdd_example(sc, FILE_IN, FILE_OUT, log)

    sc.stop()
