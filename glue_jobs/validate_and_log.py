from pyspark.sql import SparkSession
from datetime import datetime
import time
import uuid

spark = SparkSession.builder.appName("ValidateAndLog").getOrCreate()

start_time = time.time()


dag_run_id = str(uuid.uuid4())
run_timestamp = datetime.now()


df_clean = spark.read.parquet("data/processed/clean/")
df_failed = spark.read.parquet("data/processed/failed/")
total_records = df_clean.count() + df_failed.count()
passed_records = df_clean.count()
failed_records = df_failed.count()

failure_percentage = (failed_records / total_records) * 100 if total_records > 0 else 0


if failed_records == 0:
    validation_status = "SUCCESS"
elif failure_percentage < 5:
    validation_status = "WARNING"
else:
    validation_status = "FAILED"


if failed_records == 0:
    error_category = "NONE"
elif failure_percentage < 5:
    error_category = "MINOR_DATA_ISSUES"
else:
    error_category = "MAJOR_DATA_FAILURE"


end_time = time.time()
execution_time_seconds = round(end_time - start_time, 2)


print(f"""
DAG RUN ID: {dag_run_id}
Timestamp: {run_timestamp}
Total: {total_records}
Passed: {passed_records}
Failed: {failed_records}
Status: {validation_status}
Error Type: {error_category}
Execution Time: {execution_time_seconds}s
""")


log_data = [
    (
        dag_run_id,
        str(run_timestamp),
        total_records,
        passed_records,
        failed_records,
        validation_status,
        error_category,
        execution_time_seconds
    )
]

columns = [
    "dag_run_id",
    "run_timestamp",
    "total_records",
    "passed_records",
    "failed_records",
    "validation_status",
    "error_category",
    "execution_time_seconds"
]

log_df = spark.createDataFrame(log_data, columns)


log_df.write.mode("append").csv("data/logs/pipeline_logs/")

print("Pipeline log written successfully")
