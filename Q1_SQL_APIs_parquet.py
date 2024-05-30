from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, StructType, IntegerType, FloatType, StringType
from pyspark.sql.functions import to_date, year, month
from pyspark.sql.functions import col
from pyspark.sql.functions import col, count, month, year, rank, substring
from pyspark.sql.window import Window


spark = SparkSession \
    .builder \
    .appName("Query 1 SQL parquet") \
    .getOrCreate()

parquet_path_1 = "hdfs://master:9000/home/user/project2024/crime_data_2010_2019_parquet"
parquet_path_2 = "hdfs://master:9000/home/user/project2024/crime_data_2020_present_parquet"

crimes_df1 = spark.read.parquet(parquet_path_1)
crimes_df2 = spark.read.parquet(parquet_path_2)

df = crimes_df1.union(crimes_df2)

df = df.withColumn("DATE OCC", to_date("DATE OCC", "MM/dd/yyyy hh:mm:ss a"))\
    .withColumn("year", year("DATE OCC"))\
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
