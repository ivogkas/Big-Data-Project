from pyspark.sql import SparkSession
import csv
import io

spark = SparkSession \
    .builder \
    .appName("Q2_RDD_csv") \
    .getOrCreate() \
    .sparkContext

def custom_csv_split(line):
    reader = csv.reader(io.StringIO(line), delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    return next(reader)


crimes1 = spark.textFile("Crime_Data_from_2010.csv") \
            .map(custom_csv_split)

crimes2 = spark.textFile("Crime_Data_from_2020.csv") \
            .map(custom_csv_split)

crimes_rdd = crimes1.union(crimes2)


def classify_timeslot(x):
    if 500 <= int(x[3]) < 1200:
        return 'Πρωί: 5.00πμ – 11.59πμ'
    elif 1200 <= int(x[3]) < 1700:
        return 'Απόγευμα: 12.00μμ – 4.59μμ'
    elif 1700 <= int(x[3]) < 2100:
        return 'Βράδυ: 5.00μμ – 8.59μμ'
    elif int(x[3]) >= 2100 or int(x[3]) < 500:
        return 'Νύχτα: 9.00μμ – 4.59πμ'


rdd = crimes_rdd.filter(lambda x: x[15] == "STREET") \
    .map(classify_timeslot) \
    .map(lambda timeslot: (timeslot, 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

print(rdd.collect())

spark.stop()




