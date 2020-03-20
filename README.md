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
```

JDBC driver: 
- https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html 
- https://jdbc.postgresql.org/download.html


## Apache Spark
```
spark-submit --master local[3] --driver-class-path postgresql-42.2.11.jar --jars ../postgresql-42.2.11.jar processdata_jdbc.py
```

## Erkenntnisse:
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
    
