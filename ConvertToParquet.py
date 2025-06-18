from pyspark.sql import SparkSession
import time

username = "stygeorgiou"

spark = SparkSession.builder.appName("CSV to Parquet Conversion").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")

job_id = sc.applicationId


# Μετατροπή αρχείου: yellow_tripdata_2015.csv
try:
    print("\n Εκκίνηση μετατροπής: yellow_tripdata_2015.csv")
    start = time.time()

    df_2015 = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")

    df_2015.show(3, truncate=False)

    output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/yellow_tripdata_2015_{job_id}"
    df_2015.coalesce(1).write.mode("overwrite").parquet(output_dir)

    end = time.time()
    print(f" Ολοκληρώθηκε σε {end - start:.2f} sec - Output: {output_dir}")

except Exception as e:
    print(f" Σφάλμα κατά τη μετατροπή του yellow_tripdata_2015.csv: {e}")


# Μετατροπή αρχείου: yellow_tripdata_2024.csv
try:
    print("\n Εκκίνηση μετατροπής: yellow_tripdata_2024.csv")
    start = time.time()

    df_2024 = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv")

    df_2024.show(3, truncate=False)

    output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/yellow_tripdata_2024_{job_id}"
    df_2024.coalesce(1).write.mode("overwrite").parquet(output_dir)

    end = time.time()
    print(f" Ολοκληρώθηκε σε {end - start:.2f} sec - Output: {output_dir}")

except Exception as e:
    print(f" Σφάλμα κατά τη μετατροπή του yellow_tripdata_2024.csv: {e}")


# Μετατροπή αρχείου: taxi_zone_lookup.csv
try:
    print("\nΕκκίνηση μετατροπής: taxi_zone_lookup.csv")
    start = time.time()

    df_zones = spark.read.option("header", "true").option("inferSchema", "true") \
        .csv("hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv")

    df_zones.show(3, truncate=False)

    output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/data/parquet/taxi_zone_lookup_{job_id}"
    df_zones.coalesce(1).write.mode("overwrite").parquet(output_dir)

    end = time.time()
    print(f" Ολοκληρώθηκε σε {end - start:.2f} sec - Output: {output_dir}")

except Exception as e:
    print(f" Σφάλμα κατά τη μετατροπή του taxi_zone_lookup.csv: {e}")

spark.stop()