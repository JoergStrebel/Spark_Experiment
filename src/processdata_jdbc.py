"""
Login postgres: postgres/<root pw>
Daten unter /var/lib/pgsql/data

jstrebel@linux-egr6:~> sudo -u postgres createuser -P -d jstrebel
Geben Sie das Passwort der neuen Rolle ein: spark


jstrebel@linux-egr6:~> sudo -u postgres createdb -O jstrebel pysparkdb
jstrebel@linux-egr6:~> psql -d pysparkdb
psql (10.10)
Geben Sie »help« für Hilfe ein.
pysparkdb=> create table testdata(idxnr integer, timedesc varchar(200));
pysparkdb=> \copy testdata from '/home/jstrebel/devel/pyspark-test/testdata.csv' (FORMAT 'csv', delimiter ',');
COPY 10000000
pysparkdb=> select * from public.testdata limit 10;

Erkenntnisse:
    - numPartitions alleine bringt nichts , Spark wird dann nur 1 Partition machen - läuft aber! D.h. selbst eine Monster-Partition führt nicht zum Speicherüberlauf, da Spark nur Einzelsätze durchreicht mittels Iteratoren. (csv-Zieldatei)
    - Man braucht schon noch die Bounds und die partitionColumn damit Spark parallelisiert
    - driver ist notwendig als Angabe
    - die Anzahl der parallelen JDBC Verbindungen wird durch die verfügbaren Cores begrenzt - 1 Core = 1 Task = 1 Partition = 1 JDBC-Verbindung. In Summe kann man wesentlich mehr Partitionen haben, aber parallel verarbeitet werden die nur im Rahmen der verfügbaren Executor Cores.
    - auch Parquet kommt mit einer Partition zurecht. Es spillt dann auf die Disk wenn ihm der Speicher fehlt.
    - Sortierung ist kein Problem, da Spark dann zwei Stages macht und das Schreiben der Zielreihenfolge in Stage 2 passiert. In Stage 1 berechnet Spark dann die Reihenfolge.

"""

from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import datetime

def jdbc_dataset_example(spark, log, file_out):

    jdbcDF = spark.read\
        .format("jdbc")\
        .option("url", "jdbc:postgresql://localhost:5432/pysparkdb")\
        .option("dbtable", "public.testdata")\
        .option("user", "jstrebel")\
        .option("password", "spark")\
        .option("driver", "org.postgresql.Driver")\
        .option("fetchsize", 10000)\
        .option("pushDownPredicate", True)\
        .option("numPartitions", 20)\
        .option("partitionColumn","idxnr")\
        .option("lowerBound",0)\
        .option("upperBound", 1000000)\
        .load()

    #jdbcDF = jdbcDF.filter(jdbcDF[0] < 10000)
    jdbcDF.explain(True)
    jdbcDF=jdbcDF.sort("idxnr", ascending=True)
    #print(jdbcDF.rdd.toDebugString())
    #print(df.rdd.toDebugString())  #only sensible for pyspark-shell
    #log.info(df.rdd.getNumPartitions())
    #jdbcDF.write.csv(file_out, mode='overwrite')
    jdbcDF.write.parquet(file_out, mode='overwrite')



now = datetime.now() # current date and time
FILE_OUT = '/home/jstrebel/devel/pyspark-test/testdata_out'+ now.strftime("%Y%m%d%H%M%S")

if __name__ == "__main__":

    sc = SparkContext(appName="Spark Data Test")
    sc.setLogLevel(logLevel="INFO")

    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)

    spark = SparkSession(sc) \
        .builder \
        .getOrCreate()

    # basic_rdd_example(sc, FILE_IN, FILE_OUT, log)
    jdbc_dataset_example(spark, log, FILE_OUT)

    sc.stop()
