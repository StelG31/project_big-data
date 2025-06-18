from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, desc
import time
import sys

def main():
    # Δημιουργία Spark session με βασικές ρυθμίσεις
    spark = SparkSession.builder \
        .appName("Query5-DF-CSV") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    try:
        # Φόρτωση δεδομένων CSV
        print("Φόρτωση δεδομένων CSV...")
        start_load = time.time()

        trips_df = spark.read.csv(
            "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv",
            header=True,
            inferSchema=True
        )
        zones_df = spark.read.csv(
            "hdfs://hdfs-namenode:9000/data/taxi_zone_lookup.csv",
            header=True,
            inferSchema=True
        )

        load_time = time.time() - start_load
        print(f"Τα δεδομένα φορτώθηκαν σε {load_time:.2f} δευτερόλεπτα")

        # Επεξεργασία ερωτήματος
        print("Επεξεργασία ερωτήματος...")
        start_process = time.time()

        result = trips_df.alias("t") \
            .join(zones_df.alias("pu"), col("t.PULocationID") == col("pu.LocationID")) \
            .join(zones_df.alias("do"), col("t.DOLocationID") == col("do.LocationID")) \
            .filter(col("pu.Zone") != col("do.Zone")) \
            .groupBy(col("pu.Zone").alias("Pickup_Zone"),
                    col("do.Zone").alias("Dropoff_Zone")) \
            .agg(count("*").alias("Total_Trips")) \
            .orderBy(desc("Total_Trips")) \
            .limit(10)

        process_time = time.time() - start_process

         # Εμφάνιση αποτελεσμάτων
        print("\nTop 10 ζεύγη ζωνών:")
        result.show(truncate=False)

        # Αποθήκευση αποτελεσμάτων
        result.write.csv(
            "hdfs://hdfs-namenode:9000/user/stygeorgiou/ergasia/outputs/dfq5_csv_files",
            header=True,
            mode="overwrite"
        )
        print("\nΤα αποτελέσματα αποθηκεύτηκαν στο HDFS")

        # Μετρήσεις επιδόσεων
        print("\nΜετρήσεις επιδόσεων:")
        print(f"Χρόνος φόρτωσης: {load_time:.2f} δευτερόλεπτα")
        print(f"Χρόνος επεξεργασίας: {process_time:.2f} δευτερόλεπτα")
        print(f"Συνολικός χρόνος: {load_time + process_time:.2f} δευτερόλεπτα")

    except Exception as e:
        print(f"Σφάλμα: {str(e)}", file=sys.stderr)
        sys.exit(1)

    finally:
        spark.stop()
        print("\nΗ εκτέλεση ολοκληρώθηκε")

if __name__ == "__main__":
    main()