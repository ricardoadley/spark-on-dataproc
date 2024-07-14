from pyspark.sql import SparkSession
from pyspark.sql.functions import count

spark = SparkSession.builder.appName("NYC Bike Trips").getOrCreate()
df = spark.read.parquet("gs://dell-dataproc-4/GCStoGCS/*.snappy.parquet", header=True, inferSchema=True)

genders = df.groupBy("gender").agg(count("gender").alias("count"))

genders.show()
