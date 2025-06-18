from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, desc
import time

def main():
    spark = SparkSession.builder \
        .appName("Query6-CSV-2exec-4cores-8GB") \
        .config("spark.executor.instances", "2") \
        .config("spark.executor.cores", "4") \
        .config("spark.executor.memory", "8g") \
        .config("spark.sql.shuffle.partitions", "32") \
        .getOrCreate()

    try:
        start_time = time.time()

        # Φόρτωση CSV δεδομένων
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

        # Query 6: Σύνολα εσόδων ανά δήμο
        result = trips_df.join(zones_df, trips_df.PULocationID == zones_df.LocationID) \
            .groupBy("Borough") \
            .agg(
                sum("fare_amount").alias("Total_Fare"),
                sum("tip_amount").alias("Total_Tips"),
                sum("tolls_amount").alias("Total_Tolls"),
                sum("extra").alias("Total_Extras"),
                sum("mta_tax").alias("Total_MTA_Tax"),
                sum("congestion_surcharge").alias("Total_Congestion"),
                sum("airport_fee").alias("Total_Airport_Fee"),
                sum("total_amount").alias("Total_Revenue")
            ) \
            .orderBy(desc("Total_Revenue"))

        # Αποτελέσματα
        print("\nTop 5 Δήμοι ανά έσοδα:")
        result.show(5, truncate=False)

        # Αποθήκευση
        result.write.csv(
            "hdfs://hdfs-namenode:9000/user/stygeorgiou/ergasia/outputs/dfq6_config1",
            header=True,
            mode="overwrite"
        )

        elapsed_time = time.time() - start_time
        print(f"\nΧρόνος εκτέλεσης (CSV): {elapsed_time:.2f} δευτερόλεπτα")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()