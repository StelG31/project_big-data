from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, udf
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType
from datetime import datetime

username = "stygeorgiou"

spark = SparkSession.builder.appName("DF Query 1 - Avg Coordinates per Hour (UDF, CSV)").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/ergasia/outputs/dfq1_udf_{job_id}"


# Ορισμός σχήματος CSV (yellow_tripdata_2015)
schema = StructType([
    StructField("VendorID", IntegerType()),
    StructField("tpep_pickup_datetime", TimestampType()),
    StructField("tpep_dropoff_datetime", TimestampType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", DoubleType()),
    StructField("pickup_longitude", DoubleType()),
    StructField("pickup_latitude", DoubleType()),
    StructField("RateCodeID", IntegerType()),
    StructField("store_and_fwd_flag", StringType()),
    StructField("dropoff_longitude", DoubleType()),
    StructField("dropoff_latitude", DoubleType()),
    StructField("payment_type", IntegerType()),
    StructField("fare_amount", DoubleType()),
    StructField("extra", DoubleType()),
    StructField("mta_tax", DoubleType()),
    StructField("tip_amount", DoubleType()),
    StructField("tolls_amount", DoubleType()),
    StructField("improvement_surcharge", DoubleType()),
    StructField("total_amount", DoubleType())
])


# Φόρτωση CSV αρχείου
df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")


# UDF για εξαγωγή ώρας
def extract_hour(dt):
    return dt.hour if dt else None

extract_hour_udf = udf(extract_hour, IntegerType())


# Φιλτράρισμα μη έγκυρων συντεταγμένων
df_clean = df.filter((col("pickup_latitude") != 0.0) & (col("pickup_longitude") != 0.0))


# Προσθήκη στήλης hour με UDF
df_with_hour = df_clean.withColumn("hour", extract_hour_udf(col("tpep_pickup_datetime")))


# Υπολογισμός μέσων τιμών ανά ώρα
df_result = df_with_hour.groupBy("hour").agg(
    avg("pickup_latitude").alias("avg_latitude"),
    avg("pickup_longitude").alias("avg_longitude")
).orderBy("hour")


# Εμφάνιση αποτελεσμάτων
df_result.show(truncate=False)


# Αποθήκευση στο HDFS
df_result.coalesce(1).write.format("csv").option("header", "true").save(output_dir)


spark.stop()