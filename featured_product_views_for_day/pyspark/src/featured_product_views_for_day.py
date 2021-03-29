""""
This application aggregates the identifies the 
top and lowest 10 products  views for the specified day
"""
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField,FloatType,\
                              IntegerType,StringType, TimestampType
from pyspark.sql.functions import col,sum
import sys
import re

debugging=True
product_views_path=None
date_to_query=None
products_data_path=None

for index,option in enumerate(sys.argv):
    if option is not None:
        if index == 1:
            product_views_path = option
        if index == 2:
            date_to_query = option
        if index == 3:
            products_data_path = option

if product_views_path is None:
    raise Exception("Path to Product Views Data Is Required as Argument 1")
if date_to_query is None:
    raise Exception("Query Date in format yyyy-mm-dd is required as Argument 2")
if products_data_path is None:
    raise Exception("Path to Product Data is required as Argument 3")

#TODO: A more rigorous date parse option would be ideal here to ensure valid dates
date_to_query_num = re.findall("\d",date_to_query)
if len(date_to_query_num) != 8:
    raise Exception("Please enter the date in the format yyyy-mm-dd")

date_to_query_num = int("".join(date_to_query_num))


print("="*32)
print("Options")
print("="*32)
print("Product Views Path",product_views_path)
print("Date",date_to_query)
print("Products Path",products_data_path)
print("="*32)

sparkSession = SparkSession.builder\
                           .appName("Top Products Per State Per Day - {0} 1.0.0".format(date_to_query_num))\
                           .config("spark.sql.shuffle.partitions",8)\
                           .getOrCreate()
# product_schema ="product_id Int, product_category_id Int, product_name String, product_description String, product_price Float, product_image String"
product_schema = StructType([
    StructField('product_id',IntegerType(),None),
    StructField('product_category_id',IntegerType(),None),
    StructField('product_name',StringType(),None),
    StructField('product_description',StringType(),None),
    StructField('product_price',FloatType(),None),
    StructField('product_image',StringType(),None)
])

# product_category_schema = StructType([

# ])

product_views_schema = StructType([
    StructField("product_id",IntegerType(),None),
    StructField("state",StringType(),None),
    StructField("count",IntegerType(),None),
    StructField("period_start",TimestampType(),None),
    StructField("hour",IntegerType(),None)
])


product_views = sparkSession.read\
                            .format('parquet')\
                            .option('path',product_views_path)\
                            .option('schema',product_views_schema)\
                            .load()

total_product_views = product_views.where("hour between {0} and {1}".format(
    date_to_query_num*100,
    date_to_query_num*100 + 24,
))\
.groupBy(
    col("product_id")
)\
.agg(
    sum(col("count")).alias("no_views")  
).cache()

top_product_views = total_product_views.orderBy(
    col('no_views').desc()
)\
.limit(10)

lowest_product_views = total_product_views.orderBy(
    col('no_views').asc()
)\
.limit(10)

products = sparkSession.read\
                       .format('json')\
                       .option('schema',product_schema)\
                       .option('path',products_data_path)\
                       .load()
products.cache()

top_product_views = top_product_views.join(products,"product_id")

if debugging:
    print("="*32)
    print("Top Products Views")
    print("="*32)
    top_product_views.show()

lowest_product_views = lowest_product_views.join(products,"product_id")

if debugging:
    print("="*32)
    print("Lowest Products Views")
    print("="*32)
    lowest_product_views.show()

#TODO Merge product Category Data
#TODO Write to File store (Append, Partitioned By Date)



