from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
import time

def main():
    spark = SparkSession.builder \
        .appName("Query3-DF-Parquet") \
        .getOrCreate()

    # Φόρτωση Parquet δεδομένων
    start_load = time.time()
    trips_df = spark.read.parquet(
        "hdfs://hdfs-namenode:9000/user/stygeorgiou/data/parquet/yellow_tripdata_2024_spark-434fb6d8c25141b8b2a08ba983880159"
    )
    zones_df = spark.read.parquet(
        "hdfs://hdfs-namenode:9000/user/stygeorgiou/data/parquet/taxi_zone_lookup_spark-434fb6d8c25141b8b2a08ba983880159"
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
    # Έλεγχος των joins με την εντολή explain
    result.explain(extended=True)

    # Αποτελέσματα και αποθήκευση
    result.show()
    result.write.csv(
        "hdfs://hdfs-namenode:9000/user/stygeorgiou/ergasia/outputs/dfq3_parquet_files",
        header=True,
        mode="overwrite"
    )

    print(f"\nΧρόνοι εκτέλεσης (Parquet):")
    print(f"Φόρτωση δεδομένων: {load_time:.2f} δευτερόλεπτα")
    print(f"Επεξεργασία: {process_time:.2f} δευτερόλεπτα")
    print(f"Τα αποτελέσματα αποθηκεύτηκαν σε: hdfs://hdfs-namenode:9000/user/stygeorgiou/ergasia/outputs/dfq3_parquet_files")

    spark.stop()

if __name__ == "__main__":
    main()    