# Apache Spark Batch
## Python

1. Clone repository into directory pyspark-test/

2. Set up Python virtual environment : 
`python3 -m venv pyspark-test`

3. Activate virtual environment `source bin/activate` 

2. Install requirements
`pip3 install -r requirements.txt `

## PosgreSQL
Login postgres: `postgres/<root pw>`

Daten unter /var/lib/pgsql/data

```{shell script}
jstrebel@linux-egr6:~> sudo -u postgres createuser -P -d jstrebel
Geben Sie das Passwort der neuen Rolle ein: spark

jstrebel@linux-egr6:~> sudo -u postgres createdb -O jstrebel pysparkdb
jstrebel@linux-egr6:~> psql -d pysparkdb
psql (10.10)
Geben Sie »help« für Hilfe ein.
pysparkdb=> create table testdata(idxnr integer, countnr integer, timedesc varchar(200));
pysparkdb=> \copy testdata from '/home/jstrebel/devel/pyspark-test/testdata.csv' (FORMAT 'csv', delimiter ',');
COPY 10000000
pysparkdb=> select * from public.testdata limit 10;
pysparkdb=> select * from pg_stat_activity;  -- check activity on the database

pysparkdb=> alter table public.testdata add column SORTTS timestamp, add column RNDSORTTS timestamp;
pysparkdb=> update public.testdata set sortts=to_timestamp(timedesc, 'YYYY-MM-DD HH24:MI:SS.US');
pysparkdb=> update public.testdata set rndsortts=sortts + random()* interval '10 hours';

```

JDBC driver: 
- https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html 
- https://jdbc.postgresql.org/download.html


## Loading data using JDBC in Apache Spark
```
cd ./Spark_Experiment/
source ./pyspark-test/bin/activate
cd ./src
spark-submit --master local[2] --executor-memory 4G --driver-class-path postgresql-42.2.11.jar --jars ../postgresql-42.2.11.jar processdata_jdbc.py
```

### Erkenntnisse:
- numPartitions alleine bringt nichts , Spark wird dann nur 1 Partition machen - 
läuft aber! D.h. selbst eine Monster-Partition führt nicht zum Speicherüberlauf, da Spark 
nur Einzelsätze durchreicht mittels Iteratoren. (csv-Zieldatei)
- Man braucht schon noch die Bounds und die partitionColumn damit Spark parallelisiert
- der Parameter "driver" in PySpark ist notwendig als Angabe bei PostgreSQL
- die Anzahl der parallelen JDBC Verbindungen wird durch die verfügbaren Cores begrenzt - 
1 Core = 1 Task = 1 Partition = 1 JDBC-Verbindung. In Summe kann man wesentlich mehr Partitionen haben, aber parallel verarbeitet werden die nur im Rahmen der verfügbaren Executor Cores.
- auch Parquet kommt mit einer Partition zurecht. Es spillt dann auf die Disk wenn ihm der 
Speicher fehlt.
- Sortierung ist kein Problem, da Spark dann zwei Stages macht und das Schreiben der 
Zielreihenfolge in Stage 2 passiert. In Stage 1 berechnet Spark dann die Reihenfolge.
- Komisches Verhalten bei der Nutzung von DB-Indizes: der Ausführungsplan über Index liefert die Daten
langsamer als ein SeqScan. Vermutung: der SeqScan ist 3-fach parallel , der Index-Scan nur 1-fach. Vermutlich ist 
die Partitionierung genau so grobgranular, dass ein SeqScan damit schneller ist.
- Wenn man rein das Herauskopieren der Daten anschaut per \COPY, dann ist der Index-Zugriff schneller, 
aber auch nur weil das Caching in der DB besser ist. Wenn die Caches nicht gefüllt sind, ist der SeqScan schneller.
Das macht Sinn, da sich der Index besser cachen lässt als der SeqScan.
- Auch ohne Partitionierung (und egal ob mit oder ohne dynamic resource allocation) gibt 
es keinen Out-of-Memory.
- Wenn die Anzahl der Partitionen zu groß ist bzw. wenn die einzelnen Partitionen zu viele und zu klein sind, 
kommt die JVM nicht mehr mit der Garbage collection mit. Das gibt auch eine OoM Exception, aber
 im Log steht schon deutlich, dass der GC überfordert war. 
- fetchsize()
  - Beim PosgreSQL JDBC Treiber hat die Vergrößerung der fetchsize keine Auswirkung auf den 
Speicherverbrauch. Eine fetchsize() von 0 sorgt dafür, dass kein DB-Cursor verwendet wird, was der default ist 
und der JDBC-Treiber alle Daten auf einmal lädt, was in Apache Spark einen OoM Error verursacht.
Eine zu hohe fetchsize z.B. 1000000 , führt auch bei PostgreSQL zu einem OoM Error. Siehe 
[Link](https://medium.com/@FranckPachot/oracle-postgres-jdbc-fetch-size-3012d494712) und [Link 2](https://recurrentnull.wordpress.com/2015/11/20/raising-the-fetch-size-good-or-bad-memory-management-in-oracle-jdbc-12c/). 
D.h. bei PostgreSQL immer die fetchsize setzen!
  - Beim Oracle JDBC Treiber werden die per fetchsize() geholten Datensätze auf der Client-Seite
  gespeichert, was bei einer zu hohen fetchsize zu einem OoM Error führt. Oracle hat per default 
  eine fetchsize von 10 und nutzt DB-Cursors, so dass die Abfrage vielleicht langsam ist, aber nicht 
  direkt abbricht.
  - Lessons Learned: 
    - Vermeide zu hohe fetchsizes, eher so 500 - 1000. Falsche fetchsizes dürften der Grund für
  viele Job-Abbrüche in Apache Spark sein. Die fetchsize bestimmt die Puffergröße im JDBC 
  ResultSet.
    - Nutze den aktuellen Oracle 12c JDBC Treiber.
- Ein normaler Lese-Schreib-Vorgang in PySpark führt zu einer konstanten, minimalen Speicherauslastung. 
    Es wird kein Festplattenspeicher verbraucht; Spark reicht die Datensätze nur durch. Wenn so ein Job wegen 
    Speichermangel scheitert, dann macht er mehr als nur Datensätze kopieren. Es macht auch keinen Unterschied,
    wieviel RAM die Exekutoren besitzen.   
- Benchmark:
  - Apache Spark: Median Dauer pro Partition mit Index und einem Core komprimiert 
  bei 200 Partitionen: 1,9 Min. 
  - Apache Spark: Median Dauer pro Partition mit parallelem SeqScan und einem Core 
  komprimiert bei 200 Partitionen: 13 Sek. ??
  - Apache Spark: Median Dauer pro Partition mit parallelem SeqScan und einem Core 
  unkomprimiert bei 100 Partitionen: 13 Sek. ??
  - Apache Spark: Median Dauer pro Partition mit parallelem SeqScan und einem Core 
    komprimiert bei 100 Partitionen: 1,3 Min. 
  - Apache Spark: Median Dauer pro Partition mit parallelem SeqScan und einem Core 
  komprimiert bei 50 Partitionen: 1,3 Min.
  - Apache Spark: Median Dauer pro Partition mit parallelem SeqScan und einem Core 
    komprimiert bei 25 Partitionen: 1,3 Min.
  - Apache Spark: Median Dauer pro Partition mit parallelem SeqScan und einem Core 
  komprimiert bei 10 Partitionen: 1,3 Min.
  - Apache Spark: Median Dauer pro Partition mit parallelem SeqScan und einem Core 
    komprimiert bei 1 Partition: 3 Min.
  - Apache Spark: Median Dauer pro Partition mit parallelem SeqScan und 2 Cores 
    komprimiert bei 2 Partition: 2 Min.
  - Apache Spark: Median Dauer pro Partition mit Index und 2 Cores 
  komprimiert bei 2 Partition: 2 Min.
  - PSQL \COPY: ca. 1,5 Min., (bei 200 Partitionen)
- --> je weniger Partitionen, desto schneller; je mehr Cores, desto schneller

## Deduplicating data in Apache Spark
First, we load the data using JDBC and then write it to Parquet (see last chapter). 

Now, we load the Parquet files, deduplicate the data and write new Parquet files. The executor memory should be smaller 
than the size of the data files. In my case, executor memory should be at 4 GB.   

```
spark-submit --master local[2] --executor-memory 4G  deduplicate_data_parquet.py
```

### Erkenntnisse:
Beim Deduplizieren benötigt Spark ungefähr den Festplattenplatz, den 
die zip-gepackten Rohdaten auch auf der Platte brauchen würden. Wenn mehrere Stages existieren,
werden die Daten neu eingelesen, aber dadurch erhöht sich der Speicherverbrauch nicht.

    
## TODO:

- look at the execution plan of mapPartitions() vs. map() and the executor usage.
- look up Bucketed Sorted Merge Joins

# Floating-point processing in Apache Spark
## 500,000 Random Values

    Naive Sum:               num1 250040.9520799919, num2 249645.8385369990
    FP Sum:                  num1 250040.9520800000, num2 249645.8385370000
    NP 64bit:                num1 250040.9520799919, num2 249645.8385369990
    NP 32bit:                num1 250039.3593750000, num2 249644.6875000000
    Apache Spark Double:     num1 250040.9520800021, num2 249645.8385369985
    Apache Spark Float:      num1 250040.9520797309, num2 249645.8385360024

Result: Apache Spark shows pretty good accuracy when double data type is used. 

## 500,000 x 0.1 as a worst case
    Naive Sum CONST:                 num1 49999.9999995529, num2 49999.9999995529
    FP Sum CONST:                    num1 50000.0000000000, num2 50000.0000000000
    NP 64bit CONST:                  num1 49999.9999995529, num2 49999.9999995529
    NP 32bit CONST:                  num1 50177.0976562500, num2 50177.0976562500
    NP longdouble CONST:             num1 50000.0000000002, num2 50000.0000000002
    Apache Spark Double CONST:       num1 49999.9999998334, num2 49999.9999998334
    Apache Spark Float CONST:        num1 50000.0007450581, num2 50000.0007450581

Result: As soon as floating point numbers need to be processed, we either need to use
special algorithms (e.g. fsum()) or high-precision (64bit) in Python and numpy. Apache Spark is pretty 
accurate also using Float data type, so no problems here.  

