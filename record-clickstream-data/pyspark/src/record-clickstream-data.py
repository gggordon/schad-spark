# bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4,org.apache.logging.log4j:log4j-core:2.7
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


kafka_bootstrap_server="0.0.0.0:9092"
kafka_topic="product_clickstream"
checkpoint_location=None
clickstream_data_path=None

for index,option in enumerate(sys.argv):
    if index == 1:
        clickstream_data_path = option
    if index == 2:
        kafka_topic = option
    if index == 3:
        kafka_bootstrap_server = option 
    if index == 4:
        checkpoint_location = option
    if index > 5:
        break

print("Configuring with arguments")
print("="*32)
print("ClickStream Data Path :",clickstream_data_path)
print("Kafka Topic :",kafka_topic)
print("Kafka Bootstrap Server :",kafka_bootstrap_server)
print("Checkpoint Location :",checkpoint_location)
print("="*32)

if clickstream_data_path is None:
    raise Exception("Clickstream Data Path is required as first argument")

sparkSession = SparkSession.builder\
                           .appName("Record ClickStream Data")\
                           .config("spark.sql.shuffle.partitions",8)\
                           .getOrCreate()

clickStream = sparkSession.readStream\
                          .format('kafka')\
                          .option('kafka.bootstrap.servers',kafka_bootstrap_server)\
                          .option('subscribe',kafka_topic)\
                          .load()

clickStream = clickStream.selectExpr("CAST(offset as bigint)","CAST(value as STRING)")

# Value: 5590|1151|67117|1352.628|1897.375|2021/03/20 18:19:48

clickStream  = clickStream.withColumn("customer_id",expr("cast(split(value,'\\\|')[0] as bigint)"))\
                          .withColumn("product_id",expr("cast(split(value,'\\\|')[1] as bigint)"))\
                          .withColumn("browser_x_pos",expr("cast(split(value,'\\\|')[3] as float)"))\
                          .withColumn("browser_y_pos",expr("cast(split(value,'\\\|')[4] as float)"))\
                          .withColumn("date_time",expr("split(value,'\\\|')[5]"))\
                          .withColumn("zip_code",expr("cast(split(value,'\\\|')[2] as int)"))

clickStream = clickStream.drop("value")

clickStreamQuery = clickStream.writeStream\
                              .format('parquet')\
                              .partitionBy('zip_code')\
                              .outputMode('append')\
                              .trigger(processingTime='1 minute')
                              
if checkpoint_location is not None:
    clickStreamQuery = clickStreamQuery.option('checkpointLocation',checkpoint_location)

clickStreamQuery = clickStreamQuery.option('path',clickstream_data_path)

streamingQuery = clickStreamQuery.start()

streamingQuery.awaitTermination()
                          

