from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month, year, rank, substring
from pyspark.sql.window import Window
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import to_date, year, month

spark = SparkSession \
    .builder \
    .appName("Query 1 DataFrame csv") \
    .getOrCreate()

crimes_schema = StructType([
    StructField("DR_NO", StringType()),
    StructField("Date Rptd", StringType()),
    StructField("DATE OCC", StringType()),
    StructField("TIME OCC", StringType()),
    StructField("AREA ", IntegerType()),
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
            .options(header=True) \
            .schema(crimes_schema) \
            .load("Crime_Data_from_2010_to_2019.csv")

crimes_df2 = spark.read.format('csv') \
    .options(header=True) \
    .schema(crimes_schema) \
    .load("Crime_Data_from_2020_to_Present.csv")

df = crimes_df1.union(crimes_df2)


# convert columns to DateType
df = df.withColumn("Date Rptd", to_date("Date Rptd", "MM/dd/yyyy hh:mm:ss a"))
df = df.withColumn("DATE OCC", to_date("DATE OCC", "MM/dd/yyyy hh:mm:ss a"))

#
df = df.withColumn("year", year("DATE OCC"))
df = df.withColumn("month", month("DATE OCC"))



# Group by year and month
monthly_crime_counts = df.groupBy("year", "month").agg(count("*").alias("crime_total"))


# Define a window specification for ranking months within each year
window_spec = Window.partitionBy("year").orderBy(col("crime_total").desc())

# Rank the months within each year
ranked_months = monthly_crime_counts.withColumn("ranking", rank().over(window_spec))

# Filter to get the top 3 months for each year
top_months = ranked_months.filter(col("ranking") <= 3)

# Sort results by year ascending and incident count descending within each year
sorted_top_months = top_months.orderBy(col("year").asc(), col("ranking").asc())


sorted_top_months.show()

spark.stop()