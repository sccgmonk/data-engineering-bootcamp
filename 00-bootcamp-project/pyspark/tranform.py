from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, StringType, TimestampType,FloatType


KEYFILE_PATH = "/opt/spark/pyspark/deb4-tranformation-with-spark.json"

# GCS Connector Path (on Spark): /opt/spark/jars/gcs-connector-hadoop3-latest.jar
# GCS Connector Path (on Airflow): /home/airflow/.local/lib/python3.9/site-packages/pyspark/jars/gcs-connector-hadoop3-latest.jar
# spark = SparkSession.builder.appName("demo") \
#     .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
#     .config("spark.memory.offHeap.enabled", "true") \
#     .config("spark.memory.offHeap.size", "5g") \
#     .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
#     .config("google.cloud.auth.service.account.enable", "true") \
#     .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
#     .getOrCreate()

spark = SparkSession.builder.appName("demo_gcs") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "5g") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("google.cloud.auth.service.account.enable", "true") \
    .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
    .getOrCreate()

# Example schema for Greenery users data
# struct_schema = StructType([
#     StructField("user_id", StringType()),
#     StructField("first_name", StringType()),
#     StructField("last_name", StringType()),
#     StructField("email", StringType()),
#     StructField("phone_number", StringType()),
#     StructField("created_at", TimestampType()),
#     StructField("updated_at", TimestampType()),
#     StructField("address_id", StringType()),
# ])

struct_schema = StructType([
    StructField("address_id", StringType()),
    StructField("address", StringType()),
    StructField("zipcode", StringType()),
    StructField("state", StringType()),
    StructField("country", StringType()),

])

struct_schema_Order = StructType([
    StructField("order_id", StringType()),
    StructField("created_at",TimestampType()),
    StructField("order_cost",FloatType()),
    StructField("shipping_cost", FloatType()),
    StructField("order_total", FloatType()),
    StructField("tracking_id", StringType()),
    StructField("shipping_service", StringType()),
    StructField("estimated_delivery_at", TimestampType()),
    StructField("delivered_at", TimestampType()),
    StructField("status", StringType()),
    StructField("user", StringType()),
    StructField("promo", StringType()),
    StructField("address", StringType()),

])
GCS_FILE_PATH = "gs://deb4-bootcamp-19/raw/greenery/addresses/addresses.csv"

GCS_FILE_PATH_order = "gs://deb4-bootcamp-19/raw/greenery/orders/orders.csv"

# df = spark.read \
#     .option("header", True) \
#     .option("inferSchema", True) \
#     .csv(GCS_FILE_PATH)

df = spark.read \
    .option("header", True) \
    .schema(struct_schema) \
    .csv(GCS_FILE_PATH)

df.show()

df.createOrReplaceTempView("YOUR_TABLE_NAME")
result = spark.sql("""
    select
        *

    from YOUR_TABLE_NAME
""")




df2 = spark.read \
    .option("header", True) \
    .schema(struct_schema_Order) \
    .csv(GCS_FILE_PATH_order)

df2.show()

df2.createOrReplaceTempView("YOUR_TABLE_NAME2")
result2 = spark.sql("""
    select
        *

    from YOUR_TABLE_NAME2
""")


OUTPUT_PATH = "gs://deb4-bootcamp-19/processed/greenery/addresses/"
result.write.mode("overwrite").parquet(OUTPUT_PATH)


OUTPUT_PATH_Order = "gs://deb4-bootcamp-19/processed/greenery/orders/"
result2.write.mode("overwrite").parquet(OUTPUT_PATH_Order)