from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StringType, StructField, DoubleType, LongType

# ✅ Docker network ke hisaab se correct broker
KAFKA_BROKERS = "kafka-broker-1:19092"

SOURCE_TOPIC = 'financial_transactions'
AGGREGATES_TOPIC = 'transaction_aggregates'

# ✅ Correct mounted paths (docker-compose ke hisaab se)
CHECKPOINT_DIR = '/opt/spark/checkpoints'

spark = (SparkSession.builder
         .appName('FinancialTransactionsProcessor')
         # .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0')
         .config('spark.jars.packages','org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0')
         .config('spark.sql.shuffle.partitions', 4)   # lightweight
         ).getOrCreate()
spark.conf.set("spark.cassandra.connection.host", "cassandra")


# ✅ Schema
transaction_schema = StructType([
    StructField('transactionId', StringType(), True),
    StructField('userId', StringType(), True),
    StructField('merchantId', StringType(), True),
    StructField('amount', DoubleType(), True),
    StructField('transactionTime', LongType(), True),
    StructField('transactionType', StringType(), True),
    StructField('location', StringType(), True),
    StructField('paymentMethod', StringType(), True),
    StructField('isInternational', StringType(), True),
    StructField('currency', StringType(), True),
])

# ✅ Kafka read
kafka_stream = (spark.readStream
                .format("kafka")
                .option("subscribe", SOURCE_TOPIC)
                .option("kafka.bootstrap.servers", KAFKA_BROKERS)
                .option("startingOffsets", "earliest")
                .load())

# ✅ Parse JSON
transaction_df = (kafka_stream
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col('value'), transaction_schema).alias("data"))
    .select("data.*")
)

# ✅ Timestamp + watermark
transactions_df = (transaction_df
    .withColumn(
        'transactionTimestamp',
        (col('transactionTime') / 1000).cast("timestamp")
    )
    .withWatermark("transactionTimestamp", "1 minute")
)

# ✅ Aggregation
aggregated_df = (transactions_df
    .groupBy("merchantId")
    .agg(
        sum("amount").alias('totalamount'),
        count("*").alias("transactioncount")
    )
)
aggregation_query = (
    aggregated_df
    .withColumn("key", col("merchantId").cast("string"))
    .withColumn(
        "value",
        to_json(
            struct(
                col("merchantId"),
                col("totalamount"),
                col("transactioncount")
            )
            # ✅ Kafka write
        ).cast("string")
    )
    .select("key", "value")
    .writeStream
    .format("kafka")
    .outputMode("complete")   # ✅ stable with aggregation
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("topic", AGGREGATES_TOPIC)
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/aggregates")
    .start()
)

def write_to_cassandra(batch_df, batch_id):
    batch_df.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="transactions_summary", keyspace="finance") \
        .mode("append") \
        .save()

query = aggregated_df.writeStream \
    .foreachBatch(write_to_cassandra) \
    .outputMode("update") \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/cassandra") \
    .start()

spark.streams.awaitAnyTermination()



