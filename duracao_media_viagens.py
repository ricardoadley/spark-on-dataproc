from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

spark = SparkSession.builder.appName("Dell AI").getOrCreate()
df = spark.read.parquet("gs://dell-dataproc-4/GCStoGCS/*.snappy.parquet", header=True, inferSchema=True)

trip_station_avg = df.groupBy("start_station_id", "end_station_id").agg(avg("tripduration").alias("avg_tripduration"))


slowest_station = trip_station_avg.orderBy(col("avg_tripduration").desc()).limit(10)

slowest_station.show()