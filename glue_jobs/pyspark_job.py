from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number, current_timestamp
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("BankingPipeline").getOrCreate()

# Read raw data
df = spark.read.csv("data/raw/", header=True, inferSchema=True)

# Deduplication
window_spec = Window.partitionBy("trans_id").orderBy(col("trans_timestamp").desc())

df_dedup = df.withColumn("rn", row_number().over(window_spec)) \
             .filter(col("rn") == 1)

# Clean records
df_clean = df_dedup.filter(
    (col("trans_id").isNotNull()) &
    (col("amount") > 0) &
    (col("trans_type").isin("DEBIT", "CREDIT")) &
    (col("trans_timestamp") <= current_timestamp())
)

# Failed records
df_failed = df_dedup.filter(
    (col("trans_id").isNull()) |
    (col("amount") <= 0) |
    (~col("trans_type").isin("DEBIT", "CREDIT")) |
    (col("trans_timestamp") > current_timestamp())
)


df_clean.write.mode("overwrite").parquet("data/processed/clean/")
df_failed.write.mode("overwrite").parquet("data/processed/failed/")
