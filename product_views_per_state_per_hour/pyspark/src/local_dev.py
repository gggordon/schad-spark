# bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4,org.apache.logging.log4j:log4j-core:2.7
import sys
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import expr,col,window


test_mode = True
kafka_bootstrap_server="0.0.0.0:9092"
kafka_topic="product_clickstream"
checkpoint_location=None
store_data_path="./data" if test_mode else None
states_data_path=None
count_window= '5 minutes' if test_mode else '1 hour'
count_interval='1 minute' if test_mode else '5 minutes'

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
if test_mode == False:
    if states_data_path is None:
        raise Exception("States Data Path is required as second argument")

if checkpoint_location is None:
    checkpoint_location = store_data_path+'checkpoint'

sparkSession = SparkSession.builder\
                           .appName("Total Product Views Per Hour Per State 1.4.1")\
                           .config('spark.sql.parquet.compression.codec','snappy')\
                           .config("spark.sql.shuffle.partitions",8)\
                           .getOrCreate()

# reduce log level to improve visibility of console output
if test_mode: 
    sparkSession.sparkContext.setLogLevel("ERROR")


if test_mode:
    states_data = [Row(zip_code=index,state="State %d" % index) for index in range(10000,100000)]
    zipStates = sparkSession.createDataFrame(states_data,schema="zip_code int, state string")
else:
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


productCounts = productStream.withWatermark('date_time',count_window).groupBy(
   window(col('date_time'),count_window,count_interval).alias('date_time'),
   col('product_id'),
   col('state'),
)\
.count()


productCounts.createOrReplaceTempView('product_view_counts')

productViewCountsWithState = sparkSession.sql("""
SELECT
  pvc.product_id,
  pvc.state,
  pvc.count,
  pvc.date_time.start as period_start,
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

query = productViewCountsWithState.writeStream

if test_mode:
    query = query.format('csv')\
                 .option("sep","|")
else:
    query = query.format('parquet')

query = query.partitionBy('hour')\
             .outputMode('append')\
             .queryName('product_views_per_state_per_hour')\
             .option('checkpointLocation',checkpoint_location)

query = query.option('path',store_data_path)

streamingQueries  =[]
streamingQueries.append(query.start())

if test_mode:
    streamingQuery2 = productCounts.writeStream\
                                   .format('console')\
                                   .outputMode('append')\
                                   .queryName('console_product_views_per_state_per_hour')\
                                   .start()
    
    streamingQueries.append(streamingQuery2)
    streamingQuery3 = productStreamOriginal.writeStream\
                                           .format('console')\
                                           .outputMode('append')\
                                           .queryName("product_stream_original")\
                                           .start()
    streamingQueries.append(streamingQuery3)
    


for streamingQuery in streamingQueries:
    streamingQuery.awaitTermination()