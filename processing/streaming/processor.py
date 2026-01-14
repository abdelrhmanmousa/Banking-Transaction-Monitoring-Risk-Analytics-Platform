import os
import requests
import happybase
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, lit, expr, to_date, hour, current_timestamp
from pyspark.sql.avro.functions import from_avro
from pyspark.storagelevel import StorageLevel

# --- CONFIGURATION ---
KAFKA_BOOTSTRAP = "broker:29092"
SCHEMA_REGISTRY = "http://schema-registry:8081"
TOPIC = "banking_transactions"
HDFS_PATH = "hdfs://namenode:9000/user/hive/warehouse/banking/transactions"
CHECKPOINT_PATH = "hdfs://namenode:9000/checkpoints/banking_risk"

def get_latest_schema(topic):
    url = "{}/subjects/{}-value/versions/latest".format(SCHEMA_REGISTRY, topic)
    try:
        res = requests.get(url)
        return res.json()['schema']
    except Exception as e:
        print("Error fetching schema: {}".format(e))
        raise e

def create_spark():
    return SparkSession.builder \
        .appName("BankingRiskAnalytics") \
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH) \
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
        .enableHiveSupport() \
        .getOrCreate()

def main():
    spark = create_spark()
    spark.sparkContext.setLogLevel("ERROR")

    # 1. Get Avro Schema
    avro_schema_str = get_latest_schema(TOPIC)

    # 2. Read Stream from Kafka
    raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # 3. Decode Avro (Skipping 5-byte Confluent header)
    decoded_df = raw_df.select(
        from_avro(expr("substring(value, 6, length(value)-5)"), avro_schema_str).alias("data")
    ).select("data.*")

    # 4. Risk Logic & Partitioning
    enriched_df = decoded_df.withColumn("risk_score", 
        lit(0) 
        + when(col("amount") > 10000, 50).otherwise(0) 
        + when(col("is_international") == True, 30).otherwise(0) 
        + when(col("location") == "Unknown", 20).otherwise(0)
    ).withColumn("is_high_risk", col("risk_score") >= 50) \
     .withColumn("event_time", (col("timestamp") / 1000).cast("timestamp")) \
     .withColumn("dt", to_date(col("event_time"))) \
     .withColumn("hr", hour(col("event_time")))

    # 5. The DUAL WRITE function (Correctly Indented)
    def dual_write_sink(batch_df, batch_id):
        # Check if batch is empty (Spark 3.0.0 compatible)
        if batch_df.limit(1).count() == 0:
            return
            
        # Persist because we use the same data for HDFS and HBase
        batch_df.persist(StorageLevel.MEMORY_AND_DISK)

        # --- Sink 1: HDFS (Historical Layer) ---
        print("Writing Batch {} to Partitioned HDFS...".format(batch_id))
        batch_df.write \
            .partitionBy("dt", "hr") \
            .format("parquet") \
            .mode("append") \
            .save(HDFS_PATH)

        # --- Sink 2: HBase (Speed Layer Alerts) ---
        alerts_df = batch_df.filter(col("is_high_risk") == True) \
                            .withColumn("proc_ts", current_timestamp())

        def send_to_hbase(partition):
            connection = None
            try:
                # Use 'hbase-thrift' as configured in docker-compose
                connection = happybase.Connection('hbase-thrift', port=9090)
                connection.open()
                table = connection.table('banking_risk')
                
                with table.batch(batch_size=100) as b:
                    for row in partition:
                        # RowKey: AccountID_Timestamp (Encoded as bytes)
                        row_key = "{}_{}".format(row.account_id, row.timestamp).encode()
                        
                        b.put(row_key, {
                            b'tx:amount': str(row.amount).encode(),
                            b'tx:risk_score': str(row.risk_score).encode(),
                            b'tx:location': str(row.location).encode(),
                            b'tx:is_high_risk': b'true',
                            b'tx:proc_ts': str(row.proc_ts).encode()
                        })
            except Exception as e:
                print("HBase Write Error: {}".format(e))
            finally:
                if connection:
                    connection.close()

        # Execute the HBase write on the workers
        alerts_df.foreachPartition(send_to_hbase)
        
        # Clean up memory
        batch_df.unpersist()

    # 6. Start the Query
    query = enriched_df.writeStream \
        .foreachBatch(dual_write_sink) \
        .trigger(processingTime='10 seconds') \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()