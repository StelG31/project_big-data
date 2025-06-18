from pyspark.sql import SparkSession
from datetime import datetime
from math import radians, sin, cos, sqrt, atan2

username = "stygeorgiou"

sc = SparkSession \
    .builder \
    .appName("RDD Query 2 - Max Haversine Distance") \
    .getOrCreate() \
    .sparkContext

sc.setLogLevel("ERROR")

job_id = sc.applicationId
output_dir = f"hdfs://hdfs-namenode:9000/user/{username}/ergasia/outputs/rddq2_{job_id}"

# Συνάρτηση Haversine (σε km)
def haversine(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c


# Φόρτωση αρχείου CSV ως RDD
lines = sc.textFile("hdfs://hdfs-namenode:9000/data/yellow_tripdata_2015.csv")

# Αφαίρεση header
header = lines.first()
data = lines.filter(lambda x: x != header)


# Εξαγωγή: (VendorID, (distance, duration))
def parse_line(line):
    try:
        fields = line.split(",")

        # Σωστά indexes βάσει yellow_tripdata_2015.csv
        vendor = int(fields[0])
        pickup_time = datetime.strptime(fields[1], "%Y-%m-%d %H:%M:%S")
        dropoff_time = datetime.strptime(fields[2], "%Y-%m-%d %H:%M:%S")
        duration = (dropoff_time - pickup_time).total_seconds() / 60

        lon1 = float(fields[5])
        lat1 = float(fields[6])
        lon2 = float(fields[9])
        lat2 = float(fields[10])

        if 0.0 in [lat1, lon1, lat2, lon2]:
            return None

        distance = haversine(lat1, lon1, lat2, lon2)
        return (vendor, (distance, duration))
    except:
        return None

parsed = data.map(parse_line).filter(lambda x: x is not None)

# Προβολή πρώτων γραμμών για έλεγχο
print(" Δείγμα parsed δεδομένων:")
print(parsed.take(5))


# Εύρεση μέγιστης απόστασης ανά Vendor
max_distances = parsed.reduceByKey(lambda a, b: a if a[0] > b[0] else b)


# Εμφάνιση τελικών αποτελεσμάτων
print("\n Μέγιστες αποστάσεις ανά VendorID:")
for row in max_distances.collect():
    print(f"VendorID: {row[0]} | Distance: {row[1][0]:.2f} km | Duration: {row[1][1]:.2f} min")


# Αποθήκευση στο HDFS
max_distances.map(lambda x: f"{x[0]},{x[1][0]:.3f},{x[1][1]:.1f}") \
    .coalesce(1) \
    .saveAsTextFile(output_dir)