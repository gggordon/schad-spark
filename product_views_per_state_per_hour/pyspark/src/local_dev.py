# bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4,org.apache.logging.log4j:log4j-core:2.7
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr,col,window


kafka_bootstrap_server="0.0.0.0:9092"
kafka_topic="product_clickstream"
checkpoint_location="/home/tree/Desktop/_resources/projects/schad/schad-spark/product_views_per_state_per_hour/pyspark/src/checkpoint"
store_data_path="/home/tree/Desktop/_resources/projects/schad/schad-spark/product_views_per_state_per_hour/pyspark/src/data"
states_data_path=None
use_mock_state_data = True

for index,option in enumerate(sys.argv):
    if index == 1 and option is not None:
        store_data_path = option
    if index == 2:
        states_data_path = option
    if index == 3:
        kafka_topic = option
    if index == 4:
        kafka_bootstrap_server = option 
    if index == 5:
        checkpoint_location = option
    if index > 6:
        break

if store_data_path is None:
    raise Exception("Data Path is required as first argument")
# if states_data_path is None:
#     raise Exception("States Data Path is required as second argument")

sparkSession = SparkSession.builder\
                           .appName("Total Product Views Per Hour Per State 1.4.1")\
                           .config('spark.sql.parquet.compression.codec','snappy')\
                           .config("spark.sql.shuffle.partitions",8)\
                           .getOrCreate()
sparkSession.sparkContext.setLogLevel("ERROR")


if use_mock_state_data:
    from pyspark.sql import Row
    states_data = [Row(zip_code=index,state="State %d" % index) for index in range(10000,100000)]
    zipStates = sparkSession.createDataFrame(states_data,schema="zip_code int, state string")
else:
    # In the event table is not located in hive
    zipCodeStates = sparkSession.read\
                         .format('parquet')\
                         .load(states_data_path)

    zipCodeStates.createOrReplaceTempView('zip_code_states')

    zipStates = sparkSession.sql("""
    SELECT DISTINCT 
        zip_code, state
    FROM
        zip_code_states
    """)

zipStates.cache()
zipStates.createOrReplaceTempView("zip_states")

clickStream = sparkSession.readStream\
                          .format('kafka')\
                          .option('kafka.bootstrap.servers',kafka_bootstrap_server)\
                          .option('subscribe',kafka_topic)\
                          .load()

clickStream = clickStream.selectExpr("CAST(value as STRING)")

productStreamOriginal  = clickStream.select(
    expr("cast(split(value,'\\\|')[1] as bigint)").alias('product_id'),
    expr("cast(split(value,'\\\|')[2] as int)").alias('zip_code'),
    expr("""
    CASE
       WHEN split(value,'\\\|')[5] RLIKE "[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}"
           THEN cast( unix_timestamp(split(value,'\\\|')[5],"yyyy/MM/dd HH:mm:ss") as timestamp)
       ELSE NULL
    END
    """).alias('date_time')
)
productStreamOriginal.createOrReplaceTempView('product_stream')
productStream = sparkSession.sql("""
SELECT
   p.product_id,
   CASE
       WHEN zs.state is null THEN 'Unknown'
       ELSE zs.state
   END as state,
   p.date_time
FROM
   product_stream p
LEFT JOIN
   zip_states zs on zs.zip_code = p.zip_code
WHERE date_time is not null
""")


productCounts = productStream.withWatermark('date_time','5 minutes').groupBy(
   window(col('date_time'),'5 minutes','1 minutes').alias('date_time'),
   col('product_id'),
   col('state'),
)\
.count()


productCounts.createOrReplaceTempView('product_view_counts')

# Ideally we would use a left join but since our click stream data is synthetic
# We will use an inner join to filter out zip codes which may not exist


productViewCountsWithState = sparkSession.sql("""
SELECT
  pvc.product_id,
  pvc.state,
  pvc.count,
  cast(pvc.date_time.start as string) as period_start,
  cast(pvc.date_time.end as string) as period_end,
  substr(
      regexp_replace(
          cast(pvc.date_time.start as string),
          "[^0-9]",
          ""
      ),
      0,
      10
  ) as hour
FROM
  product_view_counts pvc
""")

query = productViewCountsWithState.writeStream\
                              .format('csv')\
                              .partitionBy('hour')\
                              .option("sep","|")\
                              .outputMode('append')\
                              .queryName('product_views_per_state_per_hour')
                              
if checkpoint_location is not None:
    query = query.option('checkpointLocation',checkpoint_location)

query = query.option('path',store_data_path)

streamingQuery = query.start()

query1 = productCounts.writeStream\
                     .format('console')\
                     .outputMode('append')\
                     .queryName('console_product_views_per_state_per_hour')

streamingQuery1 = query1.start()

streamingQuery2 = productStreamOriginal.writeStream.format('console').outputMode('append').queryName("product_strea_original").start()

streamingQuery.awaitTermination()


streamingQuery1.awaitTermination()

streamingQuery2.awaitTermination()
                          

