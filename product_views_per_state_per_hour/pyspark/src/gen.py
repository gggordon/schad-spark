# #TODO:

# # Retrieve aallproducts from hdfs csv
# # retrieve all states from states.csv

# For each product
#     for each state
#         for each 5 minute difference
#             generate product_id, state, count, period_start ( 5 minute timestamp), hour from timestamp

# # pvc.product_id,
# #   pvc.state,
# #   pvc.count,
# #   pvc.date_time.start as period_start,
# #   substr(
# #       regexp_replace(
# #           cast(pvc.date_time.start as string),
# #           "[^0-9]",
# #           ""
# #       ),
# #       0,
# #       10
# #   ) as hour


from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType,StructField,IntegerType,StringType,TimestampType,FloatType
from datetime import datetime,timedelta
import sys

date_gen_start_date = datetime.today() - timedelta(days=30)
time_increment = timedelta(minutes=5)
current_date_gen_time = date_gen_start_date

products_json_path = None
states_csv_path = None

for index,option in enumerate(sys.argv):
    if option is not None:
        if index == 1:
            states_csv_path = option
        if index == 2:
            products_json_path = option
    
    if index > 3:
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
                           .appName("Product Views Generator 1.0.0")\
                           .config("spark.sql.shuffle.partitions",1)\
                           .getOrCreate()


products = sparkSession.read\
                          .format('json')\
                          .option('schema',product_schema)\
                          .option('path',products_json_path)\
                          .load()\
                          .selectExpr("product_id")

products.show()

zipStates = sparkSession.read\
                        .format('csv')\
                        .option('sep',',')\
                        .option('schema',state_schema)\
                        .option('path',states_csv_path)\
                        .option('header',True)\
                        .load()\
                        .selectExpr("zip_code","state")\
                        .distinct()

zipStates.show()
