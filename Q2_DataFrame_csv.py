from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import col, count, month, year, rank, substring, when, hour

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

df = df.withColumn('hour', substring(df['TIME OCC'], 1, 2))
df = df.withColumn('hour', df['hour'].cast('int'))

df = df.withColumn('timeslots',
                   when((col("hour") >= 5) & (col("hour") < 12), 'Πρωί: 5.00πμ – 11.59πμ')
                   .when((col("hour") >= 12) & (col("hour") < 17), 'Απόγευμα: 12.00μμ – 4.59μμ')
                   .when((col("hour") >= 17) & (col("hour") < 21), 'Βράδυ: 5.00μμ – 8.59μμ')
                   .otherwise('Νύχτα: 9.00μμ – 4.59πμ'))

crime_timeslots = df.filter(df['Premis Desc'] == 'STREET')\
    .groupBy('timeslots').count() \
    .withColumnRenamed('count', 'total_crime')\
    .orderBy('total_crime', ascending=False)

crime_timeslots.show()

spark.stop()