# bin/pyspark --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.4,org.apache.logging.log4j:log4j-core:2.7
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr


kafka_bootstrap_server="0.0.0.0:9092"
kafka_topic="product_clickstream"
checkpoint_location=None
clickstream_data_path=None

for index,option in enumerate(sys.argv):
    if index == 0:
        clickstream_data_path = option
    if index == 1:
        kafka_topic = option
    if index == 2:
        kafka_bootstrap_server = option 
    if index == 4:
        checkpoint_location = option
    if index > 4:
        break

if clickstream_data_path is None:
    raise Exception("Clickstream Data Path is required as first argument")

sparkSession = SparkSession.builder\
                           .appName("Record ClickStream Data")\
                           .getOrCreate()

clickStream = sparkSession.readStream\
                          .format('kafka')\
                          .option('kafka.bootstrap.servers',kafka_bootstrap_server)\
                          .option('subscribe',kafka_topic)\
                          .load()

clickStream = clickStream.selectExpr("CAST(value as STRING)")

clickStream  = clickStream.withColumn("customer_id",expr("split('|',value)[0]"))\
                          .withColumn("product_id",expr("split('|',value)[1]"))\
                          .withColumn("zip_code",expr("split('|',value)[2]"))\
                          .withColumn("browser_x_pos",expr("split('|',value)[3]"))\
                          .withColumn("browser_y_pos",expr("split('|',value)[4]"))\
                          .withColumn("date_time",expr("cast(split('|',value)[5] as timestamp)"))

clickStream = clickStream.drop("value")

clickStreamQuery = clickStream.writeStream\
                              .format('csv')\
                              .outputMode('append')
                              
if checkpoint_location is not None:
    clickStreamQuery = clickStreamQuery.option('checkpointLocation',checkpoint_location)

clickStreamQuery = clickStreamQuery.option('path',clickstream_data_path)

clickStreamQuery.start()
                          

