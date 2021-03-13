# bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4,org.apache.logging.log4j:log4j-core:2.7
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr,col,window


kafka_bootstrap_server="0.0.0.0:9092"
kafka_topic="product_clickstream"
checkpoint_location=None
store_data_path=None

for index,option in enumerate(sys.argv):
    if index == 0:
        store_data_path = option
    if index == 1:
        kafka_topic = option
    if index == 2:
        kafka_bootstrap_server = option 
    if index == 4:
        checkpoint_location = option
    if index > 4:
        break

if store_data_path is None:
    raise Exception("Data Path is required as first argument")

sparkSession = SparkSession.builder\
                           .appName("Total Product Views Each Epoch")\
                           .option('spark.sql.parquet.compression.codec','snappy')\
                           .enableHiveSupport()\
                           .getOrCreate()

# In the event table is not located in hive
# zipCodeStates = spark.read\
#                      .format('csv')\
#                      .option('sep','|')\
#                      .option('header',True)\
#                      .path('path_to_zip_code_states')
# zipCodeStates.createOrReplaceTempView('zip_code_states')

zipStates = sparkSession.sql("""
SELECT DISTINCT 
    zip_code, state
FROM
    retaildb.states
""")
zipStates.cache()
zipStates.createOrReplaceTempView("zip_states")

clickStream = sparkSession.readStream\
                          .format('kafka')\
                          .option('kafka.bootstrap.servers',kafka_bootstrap_server)\
                          .option('subscribe',kafka_topic)\
                          .load()

clickStream = clickStream.selectExpr("CAST(value as STRING)")
#TODO: Window every hour
productStream  = clickStream.select(
    expr("split('|',value)[1]").alias('product_id'),
    expr("split('|',value)[2]").alias('zip_code'),
    expr("cast(split('|',value)[5] as timestamp)").alias('date_time')
)

# productStream = productStream.select(
#     col('product_id'),
#     col('zip_code'),
#     col('date_time'),
#     expr("""
#     concat(
#         split(' ',date_time)[0],
#         ' ',
#         split(':',split(' ',date_time)[1])[0]
#     )
#     """.trim()).alias('hour')
# )

#productStream.createOrReplaceTempView('product_state_hour')

# productCounts = sparkSession.sql("""
# SELECT
#   count(1) as no_views,
#   product_id,
#   zip_code,
#   hour
# FROM 
#   product_state_hour
# GROUP BY
#   product_id, zip_code, hour
# """)

productCounts = productStream.withWatermark('date_time','1 hour').groupBy(
   window(col('date_time'),'1 hour','5 minutes'),
   col('product_id'),
   col('zip_code'),
)\
.count()\
.withColumn('hour',     expr("""
    concat(
        split(' ',date_time)[0],
        ' ',
        split(':',split(' ',date_time)[1])[0]
    )
    """.trim())

# productCounts.createOrReplaceTempView('product_view_counts')

# Ideally we would use a left join but since our click stream data is synthetic
# We will use an inner join to filter out zip codes which may not exist
# productViewCountsWithState = sparkSession.sql("""
# SELECT
#   pvc.*,
#   zs.state
# FROM
#   product_view_counts pvc
# INNER JOIN 
#   zip_states zs ON zs.zip_code = pvc.zip_code
# """)
productViewCountsWithState = productCounts.join(zipStates,'zip_code')


                          

query = productViewCountsWithState.writeStream\
                              .format('parquet')\
                              .partitionBy('hour')\
                              .outputMode('update')
                              
if checkpoint_location is not None:
    query = query.option('checkpointLocation',checkpoint_location)

query = query.option('path',store_data_path)

query.start()
                          

