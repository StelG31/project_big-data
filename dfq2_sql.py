from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from math import radians, sin, cos, sqrt, atan2
import time

# Αρχικοποίηση Spark Session
spark = SparkSession.builder \
    .appName("NYC Taxi Q2 - Pure SQL Implementation") \
    .getOrCreate()

# Φόρτωση δεδομένων από CSV
start_time = time.time()
yellow_2015 = spark.read.csv(
    "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv",
    header=True,
    inferSchema=True
)
load_time = time.time() - start_time
print(f"Χρόνος φόρτωσης δεδομένων: {load_time:.2f} δευτερόλεπτα")

# Δημιουργία Temporary View
yellow_2015.createOrReplaceTempView("yellow_trips")

# SQL Query χωρίς UDF (Haversine υπολογισμός μέσα στο query)
query = """
WITH valid_trips AS (
    SELECT
        VendorID,
        tpep_pickup_datetime,
        tpep_dropoff_datetime,
        pickup_latitude,
        pickup_longitude,
        dropoff_latitude,
        dropoff_longitude,
        (unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)) / 60.0 AS duration_minutes
    FROM yellow_trips
    WHERE pickup_latitude != 0 AND pickup_longitude != 0
      AND dropoff_latitude != 0 AND dropoff_longitude != 0
      AND tpep_pickup_datetime < tpep_dropoff_datetime
),

trips_with_distance AS (
    SELECT
        VendorID,
        duration_minutes,
        -- Haversine formula implementation directly in SQL
        6371 * 2 * ASIN(
            SQRT(
                POWER(SIN(RADIANS(dropoff_latitude - pickup_latitude) / 2), 2) +
                COS(RADIANS(pickup_latitude)) *
                COS(RADIANS(dropoff_latitude)) *
                POWER(SIN(RADIANS(dropoff_longitude - pickup_longitude) / 2), 2)
            )
        ) AS haversine_distance_km
    FROM valid_trips
),

max_distances AS (
    SELECT
        VendorID,
        MAX(haversine_distance_km) AS max_distance_km
    FROM trips_with_distance
    GROUP BY VendorID
)

SELECT
    d.VendorID,
    ROUND(d.max_distance_km, 2) AS `Max Haversine Distance (km)`,
    ROUND(t.duration_minutes, 1) AS `Duration (min)`
FROM max_distances d
JOIN trips_with_distance t
  ON d.VendorID = t.VendorID AND d.max_distance_km = t.haversine_distance_km
ORDER BY d.VendorID
"""

# Εκτέλεση Query
start_query_time = time.time()
result = spark.sql(query)
result.show()
query_time = time.time() - start_query_time
print(f"Χρόνος εκτέλεσης query: {query_time:.2f} δευτερόλεπτα")

# Αποθήκευση αποτελεσμάτων σε CSV
result.write.csv(
    "hdfs://hdfs-namenode:9000/user/stygeorgiou/ergasia/outputs/dfq2_sql_results.csv",
    header=True,
    mode="overwrite"
)

# Προαιρετικά: Εμφάνιση πλάνου εκτέλεσης
print("Πλάνο εκτέλεσης:")
result.explain(extended=True)