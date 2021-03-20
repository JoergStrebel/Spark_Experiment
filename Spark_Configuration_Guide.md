# Configuration of Apache Spark for Oracle RDBMS
## Data Model
source table in Oracle RDBMS schema with the following table definition:
- col1: primary key
- col2..colN: data columns
- colN+1: indexed column e.g. a foreign key to some master data table. The column values might be heavily skewed.
- colN+2: update timestamp of table row, also indexed. The column values might be heavily skewed.

Database properties:
1. Size of the table roughly >1 billion rows.
1. Database only offers limited time windows for loading (hard limit)
1. Number of parallel db connections is limited (soft limit).

## Apache Spark details
- initial load: the initial load can only run in batches, as the data volume is too big to load it in one go.
    1. We pick a date that serves as the endpoint of the time period of the initial load. So we load data 
       from the start of time up to that point. This start time needs to be a predicate that is always pushed down 
       to the database as a WHERE condition in the Spark-generate SQL statement.  
    2. We use an indexed column to calculate a hash value (e.g. by using ora_hash() or mod()) and we define a 
    number of buckets in that way, such that they included a number of records that can be processed within the 
       time window.
    3. The hash column is then the partition column in Spark, the numPartitions are the number of buckets and the 
    bounds are given by the range of the hash function values. Apache Spark will then emit SQL statements that 
       include WHERE conditions to load one partition per task. 
       In essence, we want Spark to put all records from DB hash value 1 into data frame partition 1, and
       DB hash value 2 into data frame partition 2 and so on.
    4. I need to load the data in batches, and there are some ways to define those batches. The issue is that I cannot 
       use the same hash column for batch definition as I used for df partitioning, as the necessary WHERE conditions
       would conflict with the WHERE conditions of the partitioned loading. So I need to choose another indexed column.       
       1. I define a value range per batch on that indexed timestamp column , i.e. a lower and upper value between which all 
          records of my batch need to be. That's technically possible but hard to implement as my batches should 
          ideally be of similar size, so that I don't have to adjust my df partitioning scheme. In this case, I would 
          have to manually calculate these ranges per batch and account for the data skew in that column (binning 
          according to the second column). So the span of each batch might be different. The big advantage: 
          WHERE conditions on that second column could be resolved by an index access, so this is very efficient on 
          the database.
        2. I calculate a second hash function on that second indexed column in such a way that one partition equals one 
           batch. The challenge would be to find a good and realistic batch size that can be loaded in the 
           time window of one day. I would then add the batch number as an additional WHERE condition to my Spark SQL. 
           This guarantees nice and equal batches, but is inefficient on the database, as the second hash column is 
           not indexed and results in a full-table scan per task / query.
- delta load: the delta load is assumed to be small enough to run in the time window of one day, so there is no 
  batching required. 
  - Partitioning: As the data volume is much smaller than the data volume of the initial load, the df partitions 
  can be fewer, so we should have a second df partitioning scheme for the delta load. We can use the same calculated 
    hash column based on the update timestamp. 
  - Time slices: we need to push down the necessary WHERE conditions that define the time slice to load per day. 
    We have to add a lower bound, which is derived from the current maximum update timestamp in the target db 
    (from the last loading run) and an
    upper bound that is the current system time. This way, we don't load records that have faulty update timestamps.
- usage of pushdown predicates for the initial load endpoint date, the batching conditions and the delta loading time 
  bounds. ATTENTION: for the predicate pushdown to work, the data type of the pushed down value needs to be compatible 
  with the data type of the db table column in the WHERE condition. We can check the success of the predicate pushdown 
  in the Spark execution plan.
- use of index columns on the source tables for partitioning columns and/or delta columns
- don't use coalesce(), unless you have a lot of small partitions e.g. after a filter condition
- don't use count() - do the count() in the database only
- partitioned loading:
  - numPartitions as the only parameter does not work, Spark will only create one partition, but the job itself works. 
    Even single monster partition does not lead to an out-of-memory situation, as Spark RDDs consist of 
    Iterators that only pass through individual records (after transforming them). 
    (Only valid for narrow transformations) 
    - You need still the Bounds and the partitionColumn for Spark to parallelize.
- The number of paralle JDBC connections is limited by the number of parallel tasks on the available cores. 
  1 Core = 1 Task = 1 Partition = 1 JDBC connection. In sum, you can have many more partitions that can be processed 
  in parallel.  
- Beim Oracle JDBC Treiber werden die per fetchsize() geholten Datensätze auf der Client-Seite gespeichert, 
  was bei einer zu hohen fetchsize zu einem OoM Error führt. Oracle hat per default 
  eine fetchsize von 10 und nutzt DB-Cursors, so dass die Abfrage vielleicht langsam ist, aber nicht direkt abbricht.
- Lessons Learned:
    - Vermeide zu hohe fetchsizes, eher so 500 - 1000. Falsche fetchsizes dürften der Grund für
          viele Job-Abbrüche in Apache Spark sein. Die fetchsize bestimmt die Puffergröße im JDBC
          ResultSet.
    - Nutze den aktuellen Oracle 12c JDBC Treiber, da er speicheroptimiert ist.
- Data deduplication and repartition() lead to a shuffle operation which is aggressively cached in Spark. This means that the whole 
dataframe needs to fit in memory or on disk. If the target dataframe is used later on, only this cached version is used, 
  so the shuffle costs are incurred only once (but Spark may read the data set from source multiple times, but without 
  needing additional caching memory).

# Special considerations for AWS Glue
Jobs with the same tupel end up on the same Apache spark environment, which can lead to unplanned resource dependencies
 among jobs and to server overload.