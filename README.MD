# S.C.H.A.D Spark Jobs

Author G. Gordon	

## Overview

This project is a part of a larger project SCHAD created by GGordon.

Developed on Spark 2.4.4


## Run Arguments
```
bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4
```
```
--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2
```
```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 record-clickstream-data.py "/data/clickstream/recordings/data" product_clickstream solar1.solarigonics.com:6667 "hdfs:/data/clickstream/recordings/checkpoint"
```

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --master=yarn --deploy-mode cluster --executor-memory 500M --num-executors 8 record-clickstream-data.py "/data/clickstream/recordings/data" product_clickstream solar1.solarigonics.com:6667 "hdfs:/data/clickstream/recordings/checkpoint"
```