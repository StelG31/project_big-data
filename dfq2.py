from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, row_number, radians, sin, cos, sqrt, atan2
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType, TimestampType

username = "stygeorgiou"

spark = SparkSession.builder.appName("DF Query 2 - Max Haversine from CSV").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/ergasia/outputs/dfq2_{job_id}"


# Ορισμός σχήματος για το CSV
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


# Φιλτράρισμα μηδενικών συντεταγμένων
df_clean = df.filter(
    (col("pickup_latitude") != 0) & (col("pickup_longitude") != 0) &
    (col("dropoff_latitude") != 0) & (col("dropoff_longitude") != 0)
)


# Υπολογισμός Haversine απόστασης (σε km)
R = 6371.0  # ακτίνα Γης σε km

df_haversine = df_clean.withColumn("dlat", radians(col("dropoff_latitude") - col("pickup_latitude"))) \
    .withColumn("dlon", radians(col("dropoff_longitude") - col("pickup_longitude"))) \
    .withColumn("a",
        sin(col("dlat") / 2) ** 2 +
        cos(radians(col("pickup_latitude"))) *
        cos(radians(col("dropoff_latitude"))) *
        sin(col("dlon") / 2) ** 2
    ).withColumn("c", 2 * atan2(sqrt(col("a")), sqrt(1 - col("a")))) \
    .withColumn("haversine_km", R * col("c"))


# Υπολογισμός διάρκειας σε λεπτά
df_full = df_haversine.withColumn("duration_min", (
    unix_timestamp("tpep_dropoff_datetime") - unix_timestamp("tpep_pickup_datetime")
) / 60)


# Εύρεση max απόστασης ανά Vendor

windowSpec = Window.partitionBy("VendorID").orderBy(col("haversine_km").desc())

df_result = df_full.withColumn("rn", row_number().over(windowSpec)) \
    .filter(col("rn") == 1) \
    .select("VendorID", "haversine_km", "duration_min")


# Εμφάνιση αποτελεσμάτων
df_result.show()


# Αποθήκευση αποτελεσμάτων στο HDFS
df_result.coalesce(1).write \
    .format("csv") \
    .option("header", "true") \
    .save(output_dir)
