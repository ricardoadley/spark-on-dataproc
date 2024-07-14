from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = SparkSession.builder.appName("Dell AI").getOrCreate()
df = spark.read.parquet("gs://dell-dataproc-4/GCStoGCS/*.snappy.parquet", header=True, inferSchema=True)

bike_trip = df.groupBy("bikeid").agg(count("bikeid").alias("trip_count"))

bikes = bike_trip.orderBy(col("trip_count").desc()).limit(10)

bikes.show()
