from pyspark.sql import SparkSession
from pyspark.sql.types import FloatType, IntegerType, StructField, StructType, StringType, TimestampType


# KEYFILE_PATH = "/opt/spark/pyspark/YOUR_KEYFILE.json"
KEYFILE_PATH = "/opt/spark/pyspark/deb4-tranformation-with-spark.json"


# GCS Connector Path (on Spark): /opt/spark/jars/gcs-connector-hadoop3-latest.jar
# GCS Connector Path (on Airflow): /home/airflow/.local/lib/python3.9/site-packages/pyspark/jars/gcs-connector-hadoop3-latest.jar
# spark = SparkSession.builder.appName("demo") \
# .config("spark.jars", "https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar") \
#     .config("spark.memory.offHeap.enabled", "true") \
#     .config("spark.memory.offHeap.size", "5g") \
#     .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
#     .config("google.cloud.auth.service.account.enable", "true") \
#     .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
#     .getOrCreate()

spark = SparkSession.builder.appName("transform") \
    .config("spark.memory.offHeap.enabled", "true") \
    .config("spark.memory.offHeap.size", "5g") \
    .config("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("google.cloud.auth.service.account.enable", "true") \
    .config("google.cloud.auth.service.account.json.keyfile", KEYFILE_PATH) \
    .getOrCreate()

# --- Data ที่ไม่มี partition

data = "promos"
GCS_FILE_PATH = f"gs://deb4-bootcamp-19/raw/greenery/{data}/{data}.csv"

struct_schema = StructType([
    StructField("promo_id", StringType()),
    StructField("discount", IntegerType()),
    StructField("status", StringType()),

])

df = spark.read \
    .option("header", True) \
    .schema(struct_schema) \
    .csv(GCS_FILE_PATH)

df.show()

df.createOrReplaceTempView(data)
result = spark.sql(f"""
    select
        *

    from {data}
""")

OUTPUT_PATH = f"gs://deb4-bootcamp-19/processed/greenery/{data}/"
result.write.mode("overwrite").parquet(OUTPUT_PATH)