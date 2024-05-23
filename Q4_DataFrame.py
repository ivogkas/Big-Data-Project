import geopy.distance
from pyspark.sql.functions import udf
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import count, round,  avg

@udf(returnType=FloatType())
def get_distance(lat1, long1, lat2, long2):
    return geopy.distance.geodesic((lat1, long1), (lat2, long2)).km


spark = SparkSession \
    .builder \
    .appName("Query 2 DataFrame csv") \
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
    .options(header=True, inferSchema=False) \
    .schema(crimes_schema) \
    .load("Crime_Data_from_2010.csv")

crimes_df2 = spark.read.format('csv') \
    .options(header=True, inferSchema=False) \
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
    .groupBy("DIVISION").agg(round(avg("distance"), 2).alias("average_distance"), count("*").alias("incidents total")) \
    .orderBy("incidents total")

print(joined_df.show(21))

spark.stop()