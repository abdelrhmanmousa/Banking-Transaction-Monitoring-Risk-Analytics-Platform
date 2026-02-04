# streams.py
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, expr, lit, when, to_date, hour , unix_timestamp, current_timestamp
)
from pyspark.sql.avro.functions import from_avro


def read_kafka(spark: SparkSession, bootstrap: str, topic: str,
              starting_offsets: str = "latest",
              max_offsets_per_trigger: int = 0) -> DataFrame:
    r = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
    )
    if max_offsets_per_trigger and max_offsets_per_trigger > 0:
        r = r.option("maxOffsetsPerTrigger", str(max_offsets_per_trigger))
        
        # keep kafka timestamp for late data handling as kafka_timestamp
    return r.load().selectExpr("Cast(value AS BINARY)","timestamp AS kafka_timestamp")


def decode_confluent_avro(raw: DataFrame, avro_schema_str: str) -> DataFrame:
    # skip 5 bytes: 1 magic + 4 schema id
    # we select data.* plus kafka_timestamp for late data handling (lag calculations)
    return raw.select(
        from_avro(expr("substring(value, 6, length(value)-5)"), avro_schema_str).alias("data"),
        col("kafka_timestamp")
    ).select("data.*" , "kafka_timestamp")


def aligned_schemas(df_list):
    """manauly align schemas for Spark 3.0.0 union
    Adds missing columns with nulls to each DataFrame in df_list
    """
    #find all unique columns across all dataframes
    all_columns = set()
    for df in df_list:
        for name in df.columns:
            all_columns.add(name)
            
    # Add missing columns with null values
    aligned_dfs = []  
    for df in df_list:
        aligned_df = df
        for column_name in all_columns:
            if column_name not in df.columns:
                aligned_df = aligned_df.withColumn(column_name, lit(None))
                
            # Ensure the column order is exactly the same   
        aligned_dfs.append(aligned_df.select(sorted(all_columns)))
     
    # 3. Perform a standard union
    res = aligned_dfs[0]
    for next_df in aligned_dfs[1:]:
        res = res.union(next_df) 
    return res    

def split_late_data(df: DataFrame, allowed_lateness_str: str):
    """
    implement SLA filter .
    Lateness = Spark processing Time - Event Time.
    """
    # Add metrics for Observability

    df_with_metrics = df.withColumn("spark_proc_time", current_timestamp()) \
        .withColumn("source_to_kafka_lag_sec", 
                    unix_timestamp(col("kafka_timestamp")) - unix_timestamp(col("event_time"))) \
        .withColumn("kafka_to_spark_lag_sec", 
                    unix_timestamp(col("spark_proc_time")) - unix_timestamp(col("kafka_timestamp"))) \
        .withColumn("total_latency_sec", 
                    unix_timestamp(col("spark_proc_time")) - unix_timestamp(col("event_time")))
        
        
    # 3. ROUTE TO DLQ based on TOTAL latency
    # Logic: 'Is this event time newer than 10 minutes ago?'

    on_time = df_with_metrics.filter(col("event_time") >= expr(f"spark_proc_time - interval {allowed_lateness_str}"))
    late = df_with_metrics.filter(col("event_time") < expr(f"spark_proc_time - interval {allowed_lateness_str}"))
    
    return on_time, late


def create_spark(app_name: str, shuffle_partitions: int) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    return spark





def add_event_time(df: DataFrame) -> DataFrame:
    # timestamp is ms epoch in your schemas
    return df.withColumn("event_time", (col("timestamp") / 1000).cast("timestamp"))


def add_dt_hr(df: DataFrame) -> DataFrame:
    return df.withColumn("dt", to_date(col("event_time"))).withColumn("hr", hour(col("event_time")))


def score_transactions(tx: DataFrame, threshold: int) -> DataFrame:
    scored = tx.withColumn(
        "risk_score",
        lit(0)
        + when(col("amount") > 10000, lit(50)).otherwise(lit(0))
        + when(col("is_international") == True, lit(30)).otherwise(lit(0))
        + when(col("location") == "Unknown", lit(20)).otherwise(lit(0))
    ).withColumn(
        "is_high_risk",
        col("risk_score") >= lit(threshold)
    )
    return scored


def join_login_then_tx(tx_scored: DataFrame, logins: DataFrame,
                       watermark_delay: str, within_window: str) -> DataFrame:
    """
    Use case: Login then Transaction within 1 minute.
    Outputs alert rows with explicit columns (no name collisions).
    """

    # Add watermarks on event_time before join
    tx_wm = tx_scored.withWatermark("event_time", watermark_delay)
    lg_wm = logins.withWatermark("event_time", watermark_delay)

    # Time constraint: tx_time between login_time and login_time + window
    joined = tx_wm.alias("t").join(
        lg_wm.alias("l"),
        on=[
            col("t.account_id") == col("l.account_id"),
            col("t.event_time").between(col("l.event_time"), expr(f"l.event_time + interval {within_window}"))
        ],
        how="inner"
    )

    # Example fraud rule: impossible travel / mismatch locations
    # (you can tighten it later: Russia -> New York etc.)
    alerts = joined.filter(col("t.location") != col("l.location")).select(
        col("t.account_id").alias("account_id"),
        col("t.event_time").alias("event_time"),
        col("t.transaction_id").alias("transaction_id"),
        col("t.amount").alias("amount"),
        col("t.location").alias("tx_location"),
        col("l.location").alias("login_location"),
        col("l.ip_address").alias("ip_address"),
        col("l.device_type").alias("device_type"),
        col("l.login_id").alias("login_id"),
        lit("LOGIN_THEN_TX").alias("alert_type"),
        lit("login location != tx location within window").alias("reason"),
        col("t.risk_score").alias("risk_score")
    )
    return alerts


def join_activity_then_tx(tx_scored: DataFrame, activity: DataFrame,
                          watermark_delay: str, within_window: str) -> DataFrame:
    """
    Use case: profile change then high transaction within 5 minutes.
    We'll flag LIMIT_CHANGE + amount >= 10000 (adjust as you like).
    """
    tx_wm = tx_scored.withWatermark("event_time", watermark_delay)
    ac_wm = activity.withWatermark("event_time", watermark_delay)

    joined = tx_wm.alias("t").join(
        ac_wm.alias("a"),
        on=[
            col("t.account_id") == col("a.account_id"),
            col("t.event_time").between(col("a.event_time"), expr(f"a.event_time + interval {within_window}"))
        ],
        how="inner"
    )

    alerts = joined.filter(
        (col("a.activity_type") == lit("LIMIT_CHANGE")) & (col("t.amount") >= lit(10000))
    ).select(
        col("t.account_id").alias("account_id"),
        col("t.event_time").alias("event_time"),
        col("t.transaction_id").alias("transaction_id"),
        col("t.amount").alias("amount"),
        col("a.activity_type").alias("activity_type"),
        col("a.old_value").alias("old_value"),
        col("a.new_value").alias("new_value"),
        lit("ACTIVITY_THEN_TX").alias("alert_type"),
        lit("limit change then high tx within window").alias("reason"),
        col("t.risk_score").alias("risk_score")
    )
    return alerts
