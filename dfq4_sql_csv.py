from pyspark.sql import SparkSession
import time

def main():
    spark = SparkSession.builder \
        .appName("Query4-SQL-CSV") \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()

    # Φόρτωση CSV
    start_load = time.time()
    trips_df = spark.read.csv(
        "hdfs://hdfs-namenode:9000/data/yellow_tripdata_2024.csv",
        header=True,
        inferSchema=True
    )
    load_time = time.time() - start_load

    # Δημιουργία View
    trips_df.createOrReplaceTempView("trips")

    # SQL Query
    query = """
    SELECT
        VendorID,
        COUNT(*) AS NightTrips
    FROM trips
    WHERE
        (HOUR(tpep_pickup_datetime) >= 23 OR
         HOUR(tpep_pickup_datetime) < 7)
    GROUP BY VendorID
    ORDER BY NightTrips DESC
    """

    # Εκτέλεση
    start_query = time.time()
    result = spark.sql(query)
    query_time = time.time() - start_query

    # Αποτελέσματα
    result.show()
    result.write.csv(
        "hdfs://hdfs-namenode:9000/user/stygeorgiou/ergasia/outputs/dfq4_sql_csv",
        header=True,
        mode="overwrite"
    )

    print(f"\nΧρόνοι (CSV):")
    print(f"Φόρτωση: {load_time:.2f}s")
    print(f"Ερώτημα: {query_time:.2f}s")
    print(f"Σύνολο: {load_time + query_time:.2f}s")

    spark.stop()

if __name__ == "__main__":
    main()