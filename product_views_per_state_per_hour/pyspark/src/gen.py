
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType,StructField,IntegerType,\
                              StringType,FloatType,ArrayType
import sys


products_json_path = None
states_csv_path = None
output_path="./data"
number_of_intervals=2
interval_diff_in_seconds=300
start_delta_seconds=-1

# Use batch mode if data size is too large for eg. being spilled to disk too frequently
# this will split and save the generated data per hour
use_batch_mode=True

for index,option in enumerate(sys.argv):
    if option is not None:
        if index == 1:
            states_csv_path = option
        if index == 2:
            products_json_path = option
        if index == 3:
            output_path = option
        if index == 4:
            try:
                number_of_intervals = int(option)
            except:
                print("Invalid option {0} passed for number of intervals. using default {1}".format(option,number_of_intervals))
        if index == 5:
            try:
                interval_diff_in_seconds = int(option)
            except:
                print("Invalid option {0} passed for intervgal diff in seconds. using default {1}".format(option,interval_diff_in_seconds))
        
    if index > 5:
        break

if states_csv_path is None:
    raise Exception("Please include the states csv path as Argument 2")
if products_json_path is None:
    raise Exception("Please include the products json path as Argument 2")

print("="*32)
print("Options")
print("="*32)
print("States Data Path",states_csv_path)
print("Products Data Path",products_json_path)
print("Number of Timestamp Intervals",number_of_intervals)
print("Difference in Intervals in Seconds",interval_diff_in_seconds)
print("="*32)
# product_schema ="product_id Int, product_category_id Int, product_name String, product_description String, product_price Float, product_image String"
product_schema = StructType([
    StructField('product_id',IntegerType(),None),
    StructField('product_category_id',IntegerType(),None),
    StructField('product_name',StringType(),None),
    StructField('product_description',StringType(),None),
    StructField('product_price',FloatType(),None),
    StructField('product_image',StringType(),None)
])

# state_schema = "id,zip_code,city,county,state"
state_schema = StructType([
    StructField("id",IntegerType(),None),
    StructField("zip_code",IntegerType(),None),
    StructField("city",StringType(),None),
    StructField("country",StringType(),None),
    StructField("state",StringType(),None)
])

sparkSession = SparkSession.builder\
                           .appName("Product Views Generator 1.0.1")\
                           .config("spark.sql.shuffle.partitions",8)\
                           .config("spark.sql.crossJoin.enabled",True)\
                           .getOrCreate()


products = sparkSession.read\
                          .format('json')\
                          .option('schema',product_schema)\
                          .option('path',products_json_path)\
                          .load()\
                          .selectExpr("product_id")
products.cache()
products.createOrReplaceTempView("products")

zipStates = sparkSession.read\
                        .format('csv')\
                        .option('sep',',')\
                        .option('schema',state_schema)\
                        .option('path',states_csv_path)\
                        .option('header',True)\
                        .load()\
                        .selectExpr("state")\
                        .distinct()
zipStates.cache()
zipStates.createOrReplaceTempView("zip_states")

def gen_last_n_timestamps_from_now(amt,interval=300,start_delta_seconds=-1):
    """
    :param amt - amount of intervals
    :param interval - time interval in seconds
    """
    from datetime import datetime,timedelta
    if start_delta_seconds < 0:
        date_gen_start_date =  datetime.today() - timedelta(seconds=amt*interval)
    else:
        date_gen_start_date =  datetime.today() - timedelta(seconds=start_delta_seconds)
    time_increment = timedelta(seconds = interval)
    current_date_gen_time = date_gen_start_date

    items=[]
    for index in range(0,amt+1):
        current_date_gen_time = current_date_gen_time + time_increment
        items.append(current_date_gen_time.isoformat().split(".")[0])
    return items

udf_gen_last_n_timestamps_from_now = udf(gen_last_n_timestamps_from_now,ArrayType(StringType()))

sparkSession.udf.register('gen_last_n_timestamps_from_now',udf_gen_last_n_timestamps_from_now)

productViewCountsWithStateQuery="""
WITH time_stamps AS (
    SELECT explode(gen_last_n_timestamps_from_now({0},{1},{2})) as stamp
),
product_views__per_state_per_hour AS (
    SELECT
      p.product_id,
      zs.state,
      floor(rand()*1000) as count,
      cast(t.stamp as timestamp) as period_start,
      substr(
          regexp_replace(
              t.stamp,
              "[^0-9]",
              ""
          ),
          0,
          10
      ) as hour
    FROM
        products p 
        CROSS JOIN zip_states zs
        CROSS JOIN time_stamps t
)
SELECT 
    *
FROM
    product_views__per_state_per_hour
WHERE 
    count > 0
ORDER BY
    hour
"""

if use_batch_mode:
    intervals_in_an_hour = ( 60*60 ) / interval_diff_in_seconds
    total_time_to_be_considered = number_of_intervals * interval_diff_in_seconds
    no_hours = total_time_to_be_considered / (60*60)
    hour_start_delta_seconds=total_time_to_be_considered
    for hour_index in range(0, no_hours):
        hour_start_delta_seconds=hour_start_delta_seconds-(hour_index*60*60)
        print("="*32)
        print("Processing batch {} of {}".format(hour_index+1,no_hours))
        print("="*32)
        productViewCountsWithState=sparkSession.sql(
            productViewCountsWithStateQuery.format(
                intervals_in_an_hour,
                interval_diff_in_seconds,
                hour_start_delta_seconds
            )
        )
        
        productViewCountsWithState.write\
                                  .format('parquet')\
                                  .partitionBy('hour')\
                                  .option('path',output_path)\
                                  .mode('append')\
                                  .save()
else:
    productViewCountsWithState=sparkSession.sql(
        productViewCountsWithStateQuery.format(
            number_of_intervals,
            interval_diff_in_seconds,
            hour_start_delta_seconds
        )
    )
    
    productViewCountsWithState.write\
                              .format('parquet')\
                              .partitionBy('hour')\
                              .option('path',output_path)\
                              .mode('append')\
                              .save()

