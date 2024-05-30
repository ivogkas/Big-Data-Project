from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col, count, month, year, rank, substring, to_date

spark = SparkSession \
    .builder \
    .appName("Query 1 SQL csv") \
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
        StructField("LON", FloatType()),
    ])


crimes_df1 = spark.read.format('csv') \
    .options(header=True, inferSchema=False) \
    .schema(crimes_schema) \
    .load("Crime_Data_from_2010.csv")

crimes_df2 = spark.read.format('csv') \
    .options(header=True, inferSchema=False) \
    .schema(crimes_schema) \
    .load("Crime_Data_from_2020.csv")

df = crimes_df1.union(crimes_df2)

df = df.withColumn("DATE OCC", to_date("DATE OCC", "MM/dd/yyyy hh:mm:ss a")) \
    .withColumn("year", year("DATE OCC")) \
    .withColumn("month", month("DATE OCC"))

# SQL view
df.createOrReplaceTempView("crimes")

query = "WITH ranked_months AS ( \
        SELECT year, month, COUNT(*) AS crime_total, ROW_NUMBER() OVER (PARTITION BY year ORDER BY COUNT(*) DESC) AS ranking \
        FROM crimes \
        GROUP BY year, month \
        ) \
        SELECT year, month, crime_total, ranking \
        FROM ranked_months \
        WHERE ranking <= 3 \
        ORDER BY year ASC, crime_total DESC;"

result = spark.sql(query)

result.show()

spark.stop()
