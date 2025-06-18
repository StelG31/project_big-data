from pyspark.sql import SparkSession
from datetime import datetime

username = "stygeorgiou"
sc = SparkSession \
    .builder \
    .appName("RDD Query 1 - Hourly Avg Coordinates") \
    .getOrCreate() \
    .sparkContext


sc.setLogLevel("ERROR")

# Διαδρομή εξόδου στο HDFS
job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/stygeorgiou/ergasia/rdd_q1_{job_id}"


# Φόρτωση του CSV ως RDD
lines = sc.textFile("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")


# Αφαίρεση επικεφαλίδας
header = lines.first()
data = lines.filter(lambda x: x != header)


# Καθαρισμός και μετασχηματισμός
def parse_line(line):
    fields = line.split(",")
    try:
        pickup_dt = datetime.strptime(fields[1], "%Y-%m-%d %H:%M:%S")
        hour = pickup_dt.hour
        lat = float(fields[5])
        lon = float(fields[4])
        if lat == 0.0 or lon == 0.0:
            return None
        return (hour, (lat, lon, 1))  # (hour, (lat_sum, lon_sum, count))
    except:
        return None

parsed = data.map(parse_line).filter(lambda x: x is not None)


# Υπολογισμός μέσων τιμών
summed = parsed.reduceByKey(lambda a, b: (
    a[0] + b[0],  # lat
        a[1] + b[1],  # lon
    a[2] + b[2]   # count
))

averaged = summed.mapValues(lambda x: (
    x[0] / x[2],  # avg_lat
    x[1] / x[2]   # avg_lon
)).sortByKey()


# Εκτύπωση για έλεγχο
for row in averaged.collect():
    print(f"Hour: {row[0]:02d} | Avg Latitude: {row[1][0]:.6f} | Avg Longitude: {row[1][1]:.6f}")


# Αποθήκευση στο HDFS
averaged.map(lambda x: f"{x[0]},{x[1][0]},{x[1][1]}") \
    .coalesce(1) \
    .saveAsTextFile(output_dir)