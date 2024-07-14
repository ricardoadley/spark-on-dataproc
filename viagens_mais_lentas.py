from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder.appName("Dell AI").getOrCreate()

df = spark.read.parquet("gs://dell-dataproc-4/GCStoGCS/*.snappy.parquet", header=True, inferSchema=True)

duration_avg = df.select(avg("tripduration").alias("avg_tripduration"))

slowest_trips = df.orderBy("tripduration", ascending=False).limit(10)

duration_avg.show()
slowest_trips.show()
