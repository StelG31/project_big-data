from pyspark.sql import SparkSession
from pyspark.sql.functions import hour, col, avg
from pyspark.sql.types import StructField, StructType, IntegerType, DoubleType, StringType, TimestampType

username = "stygeorgiou"
spark = SparkSession \
    .builder \
    .appName("DF Query 1 - Hourly Avg Coordinates") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")

job_id = spark.sparkContext.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/ergasia/outputs/dfq1_{job_id}"


# Ορισμός σχήματος δεδομένων
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


# Φόρτωση του CSV σε DataFrame
df = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema) \
    .load("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")

# Φιλτράρισμα μη έγκυρων συντεταγμένων
df_clean = df.filter((col("pickup_latitude") != 0) & (col("pickup_longitude") != 0))


# Προσθήκη στήλης ώρας
df_with_hour = df_clean.withColumn("hour", hour("tpep_pickup_datetime"))


# Υπολογισμός μέσων τιμών
df_result = df_with_hour.groupBy("hour").agg(
    avg("pickup_latitude").alias("avg_latitude"),
    avg("pickup_longitude").alias("avg_longitude")
).orderBy("hour")


# Προβολή δείγματος αποτελεσμάτων
df_result.show(truncate=False)

# Αποθήκευση στο HDFS ως CSV
df_result.coalesce(1).write \
    .format("csv") \
    .option("header", "true") \
    .save(output_dir)