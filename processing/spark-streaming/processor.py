# processor.py
from pyspark.sql.functions import lit
from config import AppConfig
from schema_registry_client import SchemaRegistryClient
from streams import (
    create_spark, read_kafka, decode_confluent_avro,
    add_event_time, add_dt_hr, score_transactions,
    join_login_then_tx, join_activity_then_tx,
    split_late_data,aligned_schemas
)
from sinks import foreach_batch_router

def main():
    cfg = AppConfig()
    sr = SchemaRegistryClient(cfg.SCHEMA_REGISTRY)
    spark = create_spark("RealTimeFraudDetection", cfg.SHUFFLE_PARTITIONS)

    # --- 1. INGESTION STAGE ---
    def ingest_topic(topic, schema_name):
        raw = read_kafka(spark, cfg.KAFKA_BOOTSTRAP, topic, max_offsets_per_trigger=cfg.MAX_OFFSETS_PER_TRIGGER)
        decoded = decode_confluent_avro(raw, sr.latest_schema(schema_name))
        return add_event_time(decoded)

    tx_stream = ingest_topic(cfg.TOPIC_TX, cfg.TOPIC_TX)
    lg_stream = ingest_topic(cfg.TOPIC_LOGINS, cfg.TOPIC_LOGINS)
    ac_stream = ingest_topic(cfg.TOPIC_ACTIVITY, cfg.TOPIC_ACTIVITY)

    # --- 2. SLA & DLQ STAGE ---
    tx_on, tx_late = split_late_data(tx_stream, cfg.LATE_ALLOWANCE)
    lg_on, lg_late = split_late_data(lg_stream, cfg.LATE_ALLOWANCE)
    ac_on, ac_late = split_late_data(ac_stream, cfg.LATE_ALLOWANCE)

    # --- 3. BUSINESS LOGIC (Scoring & Joins) ---
    # Silver: Transactions ready for analytics
    tx_silver = score_transactions(tx_on, cfg.HIGH_RISK_THRESHOLD)

    # Alerts: Complex fraud detection
    login_alerts = join_login_then_tx(tx_silver, lg_on, cfg.WATERMARK_DELAY, cfg.LOGIN_TX_WINDOW)
    activity_alerts = join_activity_then_tx(tx_silver, ac_on, cfg.WATERMARK_DELAY, cfg.ACTIVITY_TX_WINDOW)

    # --- 4. UNIFICATION (The Tagging Factory) ---
    # We use add_dt_hr only ONCE here to keep it clean
    streams_to_union = [
       add_dt_hr(tx_on).withColumn("_type", lit("BRONZE_TX")),
        add_dt_hr(lg_on).withColumn("_type", lit("BRONZE_LOGIN")),
        add_dt_hr(ac_on).withColumn("_type", lit("BRONZE_ACTIVITY")),
        add_dt_hr(tx_silver).withColumn("_type", lit("SILVER_TX")),
        add_dt_hr(login_alerts).withColumn("_type", lit("ALERT")),
        add_dt_hr(activity_alerts).withColumn("_type", lit("ALERT")),
        add_dt_hr(tx_late).withColumn("_type", lit("LATE_TX")),
        add_dt_hr(lg_late).withColumn("_type", lit("LATE_LOGIN")),
        add_dt_hr(ac_late).withColumn("_type", lit("LATE_ACTIVITY"))
    ]
    unified_stream = aligned_schemas(streams_to_union)
    

    # --- 5. SINK STAGE ---
    # Pass the WHOLE config object to keep the function signature small
    sink_fn = foreach_batch_router(cfg)

    query = (
        unified_stream.writeStream
        .foreachBatch(sink_fn)
        .trigger(processingTime=cfg.TRIGGER_INTERVAL)
        .option("checkpointLocation", cfg.CHECKPOINT_SILVER_TX)
        .start()
    )

    query.awaitTermination()

if __name__ == "__main__":
    main()