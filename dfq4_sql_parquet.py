from pyspark.sql import SparkSession
import time

def main():
    spark = SparkSession.builder \
        .appName("Query4-SQL-Parquet") \
        .config("spark.sql.parquet.filterPushdown", "true") \
        .getOrCreate()

    # Φόρτωση Parquet
    trips_df = spark.read.parquet("hdfs://hdfs-namenode:9000/user/stygeorgiou/data/parquet/yellow_tripdata_2024_spark-434fb6d8c25141b8b2a08ba983880159")
    trips_df.createOrReplaceTempView("trips")

    # Εκτέλεση Query
    query = """
    SELECT
        VendorID,
        COUNT(*) AS NightTrips
    FROM trips
    WHERE (HOUR(tpep_pickup_datetime) >= 23 OR HOUR(tpep_pickup_datetime) < 7)
    GROUP BY VendorID
    ORDER BY NightTrips DESC
    """
    result = spark.sql(query)

    # Αποθήκευση ΩΣ CSV (για αναγνωσιμότητα)
    result.write.csv(
        "hdfs://hdfs-namenode:9000/user/stygeorgiou/ergasia/outputs/dfq4_sql_parquet",
        header=True,
        mode="overwrite"
    )

    # Εμφάνιση αποτελεσμάτων
    print("\nΑποτελέσματα:")
    result.show()

    spark.stop()

if __name__ == "__main__":
    main()