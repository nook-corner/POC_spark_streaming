from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, lit, current_timestamp
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import requests
import logging
import time

spark = SparkSession.builder \
    .appName("Kafka-Databricks-Streaming") \
    .getOrCreate()

#Kafka Config
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "45.33.103.86:9092") \
    .option("subscribe", "mock_txn_v1") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.group.id", "spark-consumer-group-1") \
    .load()


cleaned_kafka_df = kafka_df.withColumn("cleaned_value", expr("substring(value, 6, length(value)-5)"))


schema_registry_url = "http://45.33.103.86:8081"
schema_subject = "mock_txn_v1-value"

def get_latest_schema():
    response = requests.get(f"{schema_registry_url}/subjects/{schema_subject}/versions/latest")
    if response.status_code == 200:
        schema_json = response.json()
        return schema_json["schema"]
    else:
        raise Exception("Failed to fetch schema from Schema Registry")

avro_schema = get_latest_schema()

decoded_df = cleaned_kafka_df.select(from_avro(col("cleaned_value"), avro_schema).alias("data")).select("data.*")

# Validation
df_valid = decoded_df.filter(
    (col("txn_id").isNotNull()) &
    (col("timestamp").isNotNull()) &
    (col("symbol").isNotNull()) &
    (col("price").isNotNull() & (col("price") > 0)) &
    (col("volume").isNotNull() & (col("volume") > 0)) &
    (col("buyer").isNotNull()) &
    (col("seller").isNotNull())
)

df_invalid = df_value.filter(
    (col("txn_id").isNull()) |
    (col("timestamp").isNull()) |
    (col("symbol").isNull()) |
    (col("price").isNull()) | (col("price") <= 0) |
    (col("volume").isNull()) | (col("volume") <= 0) |
    (col("buyer").isNull()) |
    (col("seller").isNull())
).withColumn("error_reason", lit("Missing or invalid values"))


#Checkpoint
checkpoint_path = "dbfs:/mnt/checkpoints/mock_txn"

def process_batch_valid(df, epoch_id):
    retries = 3
    for attempt in range(retries):
        try:
            df.write.format("delta") \
                .mode("append") \
                .option("checkpointLocation", checkpoint_path) \
                .saveAsTable("mock_txn_staging")
            break
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(5)
    else:
        logging.error("All retry attempts failed for mock_txn_staging")


def process_batch_invalid(df, epoch_id):
    retries = 3
    for attempt in range(retries):
        try:
            df.write.format("delta") \
                .mode("append") \
                .option("checkpointLocation", checkpoint_path + "_dlq") \
                .saveAsTable("mock_txn_dlq")
            break
        except Exception as e:
            logging.error(f"Attempt {attempt + 1} failed: {e}")
            time.sleep(5)
    else:
        logging.error("All retry attempts failed for mock_txn_dlq")

query_valid = df_valid.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch_valid) \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", checkpoint_path) \
    .start()

query_invalid = df_invalid.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch_invalid) \
    .trigger(processingTime="10 seconds") \
    .option("checkpointLocation", checkpoint_path + "_dlq") \
    .start()

query_valid.awaitTermination()
query_invalid.awaitTermination()
