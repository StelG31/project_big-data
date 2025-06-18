from pyspark.sql import SparkSession
import time

def main():
    spark = SparkSession.builder \
        .appName("Query3-SQL-CSV") \
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

    # Δημιουργία temporary views
    trips_df.createOrReplaceTempView("trips")
    zones_df.createOrReplaceTempView("zones")

    # SQL Query
    query = """
    SELECT
        pu.Borough,
        COUNT(*) AS TotalTrips
    FROM trips t
    JOIN zones pu ON t.PULocationID = pu.LocationID
    JOIN zones do ON t.DOLocationID = do.LocationID
    WHERE pu.Borough = do.Borough
    GROUP BY pu.Borough
    ORDER BY TotalTrips DESC
    """

    # Εκτέλεση και αποθήκευση
    start_process = time.time()
    result = spark.sql(query)
    process_time = time.time() - start_process

    result.show()
    result.write.csv(
        "hdfs://hdfs-namenode:9000/user/stygeorgiou/ergasia/outputs/dfq3_sql_csv",
        header=True,
        mode="overwrite"
    )

    print(f"\nΧρόνοι εκτέλεσης (CSV):")
    print(f"Φόρτωση δεδομένων: {load_time:.2f} δευτερόλεπτα")
    print(f"Επεξεργασία: {process_time:.2f} δευτερόλεπτα")
    print(f"Τα αποτελέσματα αποθηκεύτηκαν σε: hdfs://hdfs-namenode:9000/user/stygeorgiou/ergasia/outputs/dfq3_sql_csv")

    spark.stop()

if __name__ == "__main__":
    main()