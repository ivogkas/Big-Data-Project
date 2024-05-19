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

# Specify the output path
#output_path = "/home/user/project2024/output/query1_result.csv"

# Save the results to a CSV file
#result.write.format("csv").option("header", "true").save(output_path)

spark.stop()