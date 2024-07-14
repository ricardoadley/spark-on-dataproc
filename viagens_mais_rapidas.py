from pyspark.sql import SparkSession
from pyspark.sql.functions import avg

spark = SparkSession.builder.appName("Dell AI").getOrCreate()
df = spark.read.parquet("gs://dell-dataproc-4/GCStoGCS/*.snappy.parquet", header=True, inferSchema=True)

filtered_df = df.filter(df.tripduration > 0)
duration_avg = filtered_df.select(avg("tripduration").alias("avg_tripduration"))

trips = filtered_df.orderBy("tripduration").limit(10)

duration_avg.show()
trips.show()
