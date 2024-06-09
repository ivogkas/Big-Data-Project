
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Convert to parquet") \
    .getOrCreate()

input_path = "hdfs://master:9000/home/user/project2024/crime_data_2010_2019.csv"
df = spark.read.csv(input_path, header=True, inferSchema=True)

output_path = "hdfs://master:9000/home/user/project2024/crime_data_2010_2019_parquet"
df.write.parquet(output_path)

spark.stop()
