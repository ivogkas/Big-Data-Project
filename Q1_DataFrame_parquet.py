from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, month, year, rank, substring
from pyspark.sql.window import Window
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import to_date, year, month

spark = SparkSession \
    .builder \
    .appName("Query 1 DataFrame parquet") \
    .getOrCreate()

parquet_path_1 = "hdfs://master:9000/home/user/project2024/crime_data_2010_2019_parquet"
parquet_path_2 = "hdfs://master:9000/home/user/project2024/crime_data_2020_present_parquet"

crimes_df1 = spark.read.parquet(parquet_path_1)
crimes_df2 = spark.read.parquet(parquet_path_2)

df = crimes_df1.union(crimes_df2)

# convert columns to DateType
df = df.withColumn("DATE OCC", to_date("DATE OCC", "MM/dd/yyyy hh:mm:ss a")) \
    .withColumn("year", year("DATE OCC")) \
    .withColumn("month", month("DATE OCC"))

window_spec = Window.partitionBy("year").orderBy(col("crime_total").desc())

monthly_crime_counts = df.groupBy("year", "month") \
    .agg(count("*").alias("crime_total")) \
    .withColumn("ranking", rank().over(window_spec)) \
    .filter(col("ranking") <= 3) \
    .orderBy(col("year").asc(), col("ranking").asc())

monthly_crime_counts.show()

spark.stop()