from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
)

from sqlalchemy import create_engine


# ======================================================
# CONFIGURATION
# ======================================================
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "purchases"

POSTGRES_URL = (
    "postgresql+psycopg2://postgres:postgres"
    "@localhost:5433/retail_streaming"
)

engine = create_engine(POSTGRES_URL)


# ======================================================
# SPARK SESSION
# ======================================================
spark = (
    SparkSession.builder
    .appName("RetailKafkaStreaming")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ======================================================
# EVENT SCHEMA
# ======================================================
schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("product_id", StringType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("quantity", IntegerType(), False),
    StructField("unit_price", DoubleType(), False),
    StructField("total_amount", DoubleType(), False),
    StructField("city", StringType(), False),
    StructField("event_time", StringType(), False),
])


# ======================================================
# READ FROM KAFKA
# ======================================================
raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS,
    )
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)


purchase_stream = (
    raw_stream
    .select(
        from_json(
            col("value").cast("string"),
            schema,
        ).alias("data")
    )
    .select("data.*")
)


# ======================================================
# WRITE EACH MICRO-BATCH TO POSTGRESQL
# ======================================================
def write_batch_to_postgres(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    pandas_df = batch_df.toPandas()

    pandas_df.to_sql(
        name="purchase_events",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
    )

    print(
        f"Batch {batch_id}: "
        f"{len(pandas_df)} events written to PostgreSQL."
    )


query = (
    purchase_stream.writeStream
    .foreachBatch(write_batch_to_postgres)
    .outputMode("append")
    .option(
        "checkpointLocation",
        "./checkpoints/purchase_events",
    )
    .start()
)

print(
    "Spark is streaming Kafka purchases into PostgreSQL. "
    "Press Ctrl+C to stop."
)

query.awaitTermination()