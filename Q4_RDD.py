from pyspark.sql import SparkSession
import csv
import io
import geopy.distance

spark = SparkSession \
    .builder \
    .appName("Q4_RDD_csv") \
    .getOrCreate() \
    .sparkContext

def custom_csv_split(line):
    # Create a CSV reader with custom settings
    reader = csv.reader(io.StringIO(line), delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    # Extract elements
    return next(reader)


crimes1 = spark.textFile("Crime_Data_from_2010.csv") \
            .map(custom_csv_split)

crimes2 = spark.textFile("Crime_Data_from_2020.csv") \
            .map(custom_csv_split)

LAPD_rdd = spark.textFile("LAPD_Police_Stations_new.csv") \
            .map(lambda x: (x.split(",")))

# Extract and remove header in one step for crimes1
#header1 = crimes1.first()

crimes_rdd = crimes1.union(crimes2)


crimes_rdd_formatted = crimes_rdd.map(lambda x: x if (x[26] != "0" and x[27] != "0") else None) \
                        .filter(lambda x: x != None) \
                        .map(lambda x: x if (len(x[16]) > 0 and x[16][0] == "1") else None) \
                        .filter(lambda x: x != None) \
                        .map(lambda x: [x[4], [x[16],x[26],x[27]]])

LAPD_rdd_formatted = LAPD_rdd.map(lambda x: [x[3], [x[1],x[4],x[5]]])

joined_rdd = crimes_rdd_formatted.join(LAPD_rdd_formatted)


def get_distance (lat1, long1, lat2, long2) :
    return geopy.distance.geodesic((lat1,long1),(lat2,long2)).km
#
#
joined_rdd = joined_rdd.map(lambda x: (
     x[0],
     x[1][0],
     x[1][1],
     get_distance(x[1][0][1], x[1][0][2], x[1][1][2], x[1][1][1]))) \
     .map(lambda x: ([x[0], (x[3],1)])) \
     .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))


print(joined_rdd.collect())