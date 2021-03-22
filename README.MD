
# S.C.H.A.D Spark Jobs

Author G. Gordon	

## Overview

This project is a part of a larger project SCHAD created by GGordon.

Developed on Spark 2.3.2

### Sample ClickStream Data

```
11460|163.62515|61410|1129.1833|1113|2020/04/26 22:02:06
4711|1924.4882|90815|1187.7152|485|2020/04/26 22:02:07
5588|1976.1198|70618|713.022|1042|2020/04/26 22:02:08
9449|420.69885|10873|553.48816|609|2020/04/26 22:02:08
1793|454.05835|98406|505.1231|146|2020/04/26 22:02:09
6022|117.14658|77389|1118.3727|562|2020/04/26 22:02:10
7399|1364.7094|81380|1151.32|875|2020/04/26 22:02:11
6748|821.0758|13179|732.9199|253|2020/04/26 22:02:12
```
See [Generator](https://github.com/gggordon/schad-clicksteam-generator) for more details.


## Spark Jobs

### Record Click Stream Data

This job retrieves the clickstream data from kafka, transforms it and stores each entry partitioned by state in parquet

This job is located in [record-clickstream-data](./record-clickstream-data)

#### Arguments

The arguments are retrieved from the cli/system arguments in the following order

- Data Storage Path [Required]: "/data/clickstream/recordings/data" 
- Kafka Topic [Optional,Default:'product_clickstream']: product_clickstream 
- Kafka Boostrap Server [Optional,Default:'0.0.0.0:9092']: solar1.solarigonics.com:6667 
- Checkpoint Location [Optional,Default:None]: "hdfs:/data/clickstream/recordings/checkpoint"

#### Dependencies
- org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4

#### Deployment
```bash

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --master=yarn --deploy-mode=cluster --executor-memory 600M --driver-memory 600M record-clickstream-data.py "/data/clickstream/recordings/data" product_clickstream solar1.solarigonics.com:6667 "hdfs:/data/clickstream/recordings/checkpoint"

```

### Product Views Per State Per Hour

This job retrieves the clickstream data from kafka, transforms it and merges it with existing state data to before counting the total product views per state and storing the results in parquet.

This job is located in [product_views_per_state_per_hour](./product_views_per_state_per_hour)

#### Arguments

The arguments are retrieved from the cli/system arguments in the following order

- Data Storage Path [Required]: "/data/clickstream/product_views_per_state_per_hour/data" 
- Parquet States Data Path [Required]: 
- Kafka Topic [Optional,Default:'product_clickstream']: product_clickstream 
- Kafka Boostrap Server [Optional,Default:'0.0.0.0:9092']: solar1.solarigonics.com:6667 
- Checkpoint Location [Optional,Default:None]: "hdfs:/data/clickstream/product_views_per_state_per_hour/checkpoint"


#### Dependencies
- org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4

#### Deployment
```bash

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.2 --master=yarn --executor-memory 1G --driver-memory 1G product_views_per_state_per_hour.py "/data/clickstream/product_views_per_state_per_hour/data" "/warehouse/tablespace/managed/hive/retaildb.db/states/base_0000001" product_clickstream solar1.solarigonics.com:6667 "hdfs:/data/clickstream/product_views_per_state_per_hour/checkpoint"

```


## License

Copyright [gggordon](https://github.com/gggordon) 2020

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0)
