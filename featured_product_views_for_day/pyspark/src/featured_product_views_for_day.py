""""
This application aggregates the identifies the 
top and lowest 10 products  views for the specified day
"""
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField,FloatType,\
                              IntegerType,StringType, TimestampType
from pyspark.sql.functions import col,sum, expr, lit
import sys
import re

debugging=False
product_views_path=None
date_to_query=None
order = "desc"
products_data_path=None
categories_data_path=None
department_data_path=None
store_data_path=None

for index,option in enumerate(sys.argv):
    if option is not None:
        if index == 1:
            product_views_path = option
        if index == 2:
            date_to_query = option
        if index == 3:
            store_data_path = option
        if index == 4 and (option == "top" or option == "low"):
            order = "desc" if option == "top" else "asc"
        if index == 5:
            products_data_path = option
        if index == 6:
            categories_data_path = option
        if index == 7:
            department_data_path = option
    if index > 7:
        break

if product_views_path is None:
    raise Exception("Path to Product Views Data Is Required as Argument 1")
if date_to_query is None:
    raise Exception("Query Date in format yyyy-mm-dd is required as Argument 2")
if products_data_path is None:
    raise Exception("Path to Data Storage Location is required as Argument 3")


print("="*32)
print("Options")
print("="*32)
print("Product Views Path",product_views_path)
print("Date",date_to_query)
print("Products Path",products_data_path)
print("="*32)

date_to_query_num = re.findall("\d",date_to_query)
# if re.match("^\d{4}-\d{2}-\d{2}$",date_to_query):
#     raise Exception("Please enter the date in the format yyyy-mm-dd ")

date_to_query_num = int("".join(date_to_query_num))




sparkSession = SparkSession.builder\
                           .appName("Top Products Per State Per Day - {0} 1.0.0".format(date_to_query_num))\
                           .config("spark.sql.shuffle.partitions",8)\
                           .getOrCreate()

if debugging:
    sparkSession.sparkContext.setLogLevel("ERROR")




if products_data_path is not None:
    # product_schema ="product_id Int, product_category_id Int, product_name String, product_description String, product_price Float, product_image String"
    product_schema = StructType([
        StructField('product_id',IntegerType(),None),
        StructField('product_category_id',IntegerType(),None),
        StructField('product_name',StringType(),None),
        StructField('product_description',StringType(),None),
        StructField('product_price',FloatType(),None),
        StructField('product_image',StringType(),None)
    ])

    products = sparkSession.read\
                           .format('json')\
                           .option('schema',product_schema)\
                           .option('path',products_data_path)\
                           .load()
    products.cache()

if categories_data_path is not None:
    #product_category_schema ="category_id Int, category_department_id Int, category_name String"

    product_category_schema = StructType([
        StructField('category_id',IntegerType(),None),
        StructField('category_department_id',IntegerType(),None),
        StructField('category_name',StringType(),None)
    ])

    categories = sparkSession.read\
                             .format('json')\
                             .option('schema',product_category_schema)\
                             .option('path',categories_data_path)\
                             .load()
    categories.cache()

if department_data_path is not None:
    # department_schema = "department_id Int, department_name String"
    department_schema = StructType([
        StructField('department_id',IntegerType(),None),
        StructField('department',StringType(),None)
    ])
    departments = sparkSession.read\
                             .format('json')\
                             .option('schema',department_schema)\
                             .option('path',department_data_path)\
                             .load()
    departments.cache()

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
product_views.createOrReplaceTempView("product_views")

if debugging:
    print("="*32)
    print("Products Views")
    print("="*32)
    product_views.show(20)
    
# since 1 hour windows recorded overlap due to 5 minute intervals we will
# extract only data for each hour to not count instances multiple times.
# We will use the last minute recorded in the hour
# Ideally these intervals should be recorded standard from the original dataset
# As a change for eg minute 59 would be recording views in the last minute of the 
# hour before
last_minute_in_hour = sparkSession.sql("""
    SELECT 
        max(cast(split(CAST(pv.period_start as string),':')[1] as int)) as max_minute
    FROM
        product_views pv
    WHERE
        pv.hour = {0} 
""".format( date_to_query_num*100)
).first().max_minute

product_views_each_hour = product_views.where("hour between {0} and {1}".format(
    date_to_query_num*100,
    date_to_query_num*100 + 24,
))\
.where("""
    CAST(period_start as string) LIKE "%:{0}:%"
""".format(last_minute_in_hour))

if debugging:
    print("="*32)
    print("Products Views Each Hour")
    print("="*32)
    product_views_each_hour.show(5)


total_product_views = product_views_each_hour.groupBy(
    col("product_id")
)\
.agg(
    sum(col("count")).alias("no_views")  
).cache()

agg_product_views = total_product_views.orderBy(
    col('no_views').desc() if order == 'desc' else col('no_views').asc()
)\
.limit(10)

if products_data_path is not None:
    agg_product_views = agg_product_views.join(products,"product_id")

    if categories_data_path is not None:
        agg_product_views = agg_product_views.join(categories,categories.category_id == products.product_category_id)
    
        if department_data_path is not None:
            agg_product_views = agg_product_views.join(departments,departments.department_id == categories.category_department_id)


agg_product_views = agg_product_views.select(
 col("product_id"),
 col("no_views"),
 col("product_category_id").alias("category_id"),
 col("department_id") if department_data_path is not None else lit(None).alias("department_id").cast("int"),
 col("product_name") if products_data_path is not None else lit(None).alias("product_name").cast("string"),
 col("product_description") if products_data_path is not None else lit(None).alias("product_description").cast("string"),
 col("product_image") if products_data_path is not None else lit(None).alias("product_image").cast("string"),
 col("product_price") if products_data_path is not None else lit(None).alias("product_price").cast("float"),
 col("category_name") if categories_data_path is not None else lit(None).alias("category_name").cast("string"),  
 col("department_name") if department_data_path is not None else lit(None).alias("department_name").cast("string"),
 lit(date_to_query).alias("date").cast("string"),
 lit(date_to_query.split("-")[2]).alias("day").cast("int"),
 lit(date_to_query.split("-")[1]).alias("month").cast("int"),
 lit(date_to_query.split("-")[0]).alias("year").cast("int"),
).orderBy(
    col('no_views').desc() if order == 'desc' else col('no_views').asc()
)

if debugging:
    print("="*32)
    print("Top Products Views")
    print("="*32)
    agg_product_views.cache()
    agg_product_views.show()

agg_product_views.repartition(1)\
                 .write\
                 .format("csv")\
                 .partitionBy("year","month","day")\
                 .mode('overwrite')\
                 .option("sep","|")\
                 .option("path",store_data_path)\
                 .option("header","true")\
                 .save()



