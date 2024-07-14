from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col

spark = SparkSession.builder.appName("Dell AI").getOrCreate()
df = spark.read.parquet("gs://dell-dataproc-4/GCStoGCS/*.snappy.parquet", header=True, inferSchema=True)

year = 2024
df = df.withColumn("age", year - col("birth_year"))

age_avg = df.select(avg("age").alias("avg_age"))

age_avg.show()
