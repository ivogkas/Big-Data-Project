from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import count, round,  avg, udf
import math


@udf(FloatType())
def get_distance(lat1, long1, lat2, long2):
    lat1, long1, lat2, long2 = map(float, [lat1, long1, lat2, long2])
    lat1, long1, lat2, long2 = map(math.radians, [lat1, long1, lat2, long2])

    dlat = lat2 - lat1
    dlon = long2 - long1
    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.asin(math.sqrt(a))
    r = 6371.0
    return c * r


spark = SparkSession \
    .builder \
    .appName("Query 4 DataFrame") \
    .getOrCreate()


crimes_schema = StructType([
        StructField("DR_NO", StringType()),
        StructField("Date Rptd", StringType()),
        StructField("DATE OCC", StringType()),
        StructField("TIME OCC", StringType()),
        StructField("AREA", IntegerType()),
        StructField("AREA NAME", StringType()),
        StructField("Rpt Dist No", IntegerType()),
        StructField("Part 1-2", IntegerType()),
        StructField("Crm Cd", IntegerType()),
        StructField("Crm Cd Desc", StringType()),
        StructField("Mocodes", StringType()),
        StructField("Vict Age", IntegerType()),
        StructField("Vict Sex", StringType()),
        StructField("Vict Descent", StringType()),
        StructField("Premis Cd", IntegerType()),
        StructField("Premis Desc", StringType()),
        StructField("Weapon Used Cd", IntegerType()),
        StructField("Weapon Desc", StringType()),
        StructField("Status", StringType()),
        StructField("Status Desc", StringType()),
        StructField("Crm Cd 1", IntegerType()),
        StructField("Crm Cd 2", IntegerType()),
        StructField("Crm Cd 3", IntegerType()),
        StructField("Crm Cd 4", IntegerType()),
        StructField("LOCATION", StringType()),
        StructField("Cross Street", StringType()),
        StructField("LAT", FloatType()),
        StructField("LON", FloatType())
    ])


LAPD_schema = StructType([
        StructField("OBJECTID", StringType()),
        StructField("DIVISION", StringType()),
        StructField("LOCATION", StringType()),
        StructField("PREC", IntegerType()),
        StructField("x", FloatType()),
        StructField("y", FloatType())
])


crimes_df1 = spark.read.format('csv') \
    .options(header=False, inferSchema=False, skipFirstRow=True) \
    .schema(crimes_schema) \
    .load("Crime_Data_from_2010.csv")

crimes_df2 = spark.read.format('csv') \
    .options(header=False, inferSchema=False, skipFirstRow=True) \
    .schema(crimes_schema) \
    .load("Crime_Data_from_2020.csv")

LAPD_df = spark.read.format('csv') \
    .options(header=True, inferSchema=False) \
    .schema(LAPD_schema) \
    .load("LAPD_Police_Stations_new.csv")

crime_df = crimes_df1.union(crimes_df2)


crime_df = crime_df.filter(
    (crime_df["LON"] != 0.0) &
    (crime_df["LAT"] != 0.0) &
    (crime_df["Weapon Used Cd"].isNotNull()) &
    (crime_df["Weapon Used Cd"] >= 100) &
    (crime_df["Weapon Used Cd"] < 200)
)

joined_df = crime_df.join(LAPD_df, crime_df["AREA"] == LAPD_df['PREC'], how="inner")

joined_df = joined_df.withColumn("distance", get_distance(joined_df["LAT"], joined_df["LON"], joined_df["y"], joined_df["x"])) \
    .groupBy("DIVISION").agg(round(avg("distance"), 3).alias("average_distance"), count("*").alias("incidents total")) \
    .orderBy("incidents total", ascending=False)

print(joined_df.show(21))
data = joined_df.collect()

with open("Q4_RDD.txt", 'w') as new_file:
    for d in data:
        resdata = ""
        for x in d:
            if type(x) == list or type(x) == tuple:
                for t in x:
                    resdata += str(t) + ", "
            else:
                resdata += str(x) + ", "
        resdata += "\n"
        new_file.write(resdata)

spark.stop()
