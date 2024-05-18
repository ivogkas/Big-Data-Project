from pyspark.sql import SparkSession
import csv
import io

sc = SparkSession \
    .builder \
    .appName("Q2_RDD_csv") \
    .getOrCreate() \
    .sparkContext

def custom_csv_split(line):
    # Create a CSV reader with custom settings
    reader = csv.reader(io.StringIO(line), delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
    # Extract elements
    return next(reader)


crimes1 = sc.textFile("Crime_Data_from_2010_to_2019.csv") \
            .map(custom_csv_split)

crimes2 = sc.textFile("Crime_Data_from_2020_to_Present.csv") \
            .map(custom_csv_split)


#header = crimes2.first()
#crimes2 = crimes2.filter(lambda row: row != header) # filter out the header

crimes_rdd = crimes1.union(crimes2)

morning_5_to_12 = crimes_rdd.map(lambda x: x if (x[15] == "STREET" and 500 <= int(x[3]) < 1200) else None) \
       .filter(lambda x: x != None) \

afternoon_12_to_17 = crimes_rdd.map(lambda x: x if (x[15] == "STREET" and 1200 <= int(x[3]) < 1700) else None) \
        .filter(lambda x: x != None) \

evening_17_to_21 = crimes_rdd.map(lambda x: x if (x[15] == "STREET" and 1700 <= int(x[3]) < 2100) else None) \
       .filter(lambda x: x != None) \

night_21_to_5 = crimes_rdd.map(lambda x: x if (x[15] == "STREET" and (int(x[3]) >= 2100 or int(x[3]) < 500)) else None) \
       .filter(lambda x: x != None) \


dict_total_crime = {"Πρωί: 5.00πμ – 11.59πμ": morning_5_to_12.count(),
                    "Απόγευμα: 12.00μμ – 4.59μμ": afternoon_12_to_17.count(),
                    "Βράδυ: 5.00μμ – 8.59μμ": evening_17_to_21.count(),
                    "Νύχτα: 9.00μμ – 4.59πμ":  night_21_to_5.count()}

sorted_data = sorted(dict_total_crime.items(), key=lambda x: x[1], reverse=True)

print(sorted_data)




