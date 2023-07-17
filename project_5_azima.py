from pyspark.sql import SparkSession
from pyspark.sql import functions
from pyspark.sql.functions import col, max, date_format, count


#create session spark
spark = SparkSession.builder.appName("Project_5_azima").getOrCreate()

#read file tripdata
df = spark.read.parquet("/home/dev/airflow/spark-code/Project-5/fhv_tripdata_2021-02.parquet")

# 1. How many taxi trips were there on February 15?
taxi_15February = df.filter((col("pickup_datetime").startswith("2021-02-15"))).count()
print(f'\n Number of taxi trips on February 15 = {taxi_15February}\n')

# 2. Find the longest trip for each day ?
df = df.withColumn("pickup_date", date_format("pickup_datetime", "yyyy-MM-dd"))
longestTrips = df.groupBy("pickup_date").agg(max("pickup_datetime").alias("max_trip_distance"))
longestTrips.show()

# 3. Find Top 5 Most frequent `dispatching_base_num` ?
dispatchBaseNum = df.groupBy("dispatching_base_num").agg(count("*").alias("count"))
mostFrequent = dispatchBaseNum.orderBy(col("count").desc())
mostFrequent.show(5)

# 4. Find Top 5 Most common location pairs (PUlocationID and DOlocationID) ?
mostCommonLocations = df.filter((col("PUlocationID").isNotNull()) & (col("DOlocationID").isNotNull()))
top5MostLocationPairs = mostCommonLocations.groupBy("PUlocationID", "DOlocationID").count().orderBy(col("count").desc())
top5MostLocationPairs.show(5)
