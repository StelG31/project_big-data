from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import time

def main():
    spark = SparkSession.builder \
        .appName("Query3-DF-CSV") \
        .getOrCreate()

    # Φόρτωση CSV δεδομένων
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

    # Επεξεργασία με DataFrame API
    start_process = time.time()
    result = trips_df.join(zones_df.alias("pu"), col("PULocationID") == col("pu.LocationID")) \
                   .join(zones_df.alias("do"), col("DOLocationID") == col("do.LocationID")) \
                   .filter(col("pu.Borough") == col("do.Borough")) \
                   .groupBy("pu.Borough") \
                   .agg(count("*").alias("TotalTrips")) \
                   .orderBy(col("TotalTrips").desc())

    process_time = time.time() - start_process

    # Αποτελέσματα και αποθήκευση
    result.show()
    result.write.csv(
        "hdfs://hdfs-namenode:9000/user/stygeorgiou/ergasia/outputs/dfq3_csv_files",
        header=True,
        mode="overwrite"
    )

    print(f"\nΧρόνοι εκτέλεσης (CSV):")
    print(f"Φόρτωση δεδομένων: {load_time:.2f} δευτερόλεπτα")
    print(f"Επεξεργασία: {process_time:.2f} δευτερόλεπτα")
    print(f"Τα αποτελέσματα αποθηκεύτηκαν σε: hdfs://hdfs-namenode:9000/user/stygeorgiou/ergasia/outputs/dfq3_csv_files")
    
    spark.stop()

if __name__ == "__main__":
    main()
