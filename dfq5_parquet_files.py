from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc
import time
import sys

def main():
    # Δημιουργία Spark session
    spark = SparkSession.builder \
        .appName("Query5-DF-Parquet") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .getOrCreate()

    try:
        # Φόρτωση δεδομένων Parquet
        print("Φόρτωση δεδομένων...")
        trips_df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/stygeorgiou/data/parquet/yellow_tripdata_2024_spark-434fb6d8c25141b8b2a08ba983880159")
        zones_df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/stygeorgiou/data/parquet/taxi_zone_lookup_spark-434fb6d8c25141b8b2a08ba983880159")

        # Επεξεργασία ερωτήματος
        print("Επεξεργασία ερωτήματος...")
        result = trips_df.alias("t") \
            .join(zones_df.alias("pu"), col("t.PULocationID") == col("pu.LocationID")) \
            .join(zones_df.alias("do"), col("t.DOLocationID") == col("do.LocationID")) \
            .filter(col("pu.Zone") != col("do.Zone")) \
            .groupBy(col("pu.Zone").alias("Pickup_Zone"),
                    col("do.Zone").alias("Dropoff_Zone")) \
            .agg(count("*").alias("Total_Trips")) \
            .orderBy(desc("Total_Trips")) \
            .limit(10)



        # Εμφάνιση αποτελεσμάτων
        print("\nΑποτελέσματα:")
        result.show(truncate=False)

        # Αποθήκευση σε CSV για αναγνωσιμότητα
        print("\nΑποθήκευση αποτελεσμάτων...")
        result.write.csv(
            "hdfs://hdfs-namenode:9000/user/stygeorgiou/ergasia/outputs/dfq5_parquet_files",
            header=True,
            mode="overwrite"
        )
        print("Τα αποτελέσματα αποθηκεύτηκαν σε μορφή CSV")

    except Exception as e:
        print(f"Σφάλμα: {str(e)}", file=sys.stderr)
        sys.exit(1)

    finally:
        spark.stop()
        print("\nΟλοκλήρωση εκτέλεσης")

if __name__ == "__main__":
    main()