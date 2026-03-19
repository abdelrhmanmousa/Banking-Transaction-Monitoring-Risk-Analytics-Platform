---- ----- - Part 1: Table Creations (The Medallion Schema)---------------
----- Run these inside Hive Beeline (docker exec -it hive-server beeline -u jdbc:hive2://localhost:10000).
-- 1. The Bronze Layer (Raw Audit Trail)
--Stores every transaction that passed the basic ingestion, including the lag metrics.
CREATE EXTERNAL TABLE IF NOT EXISTS bronze_transactions (
    transaction_id       STRING,
    account_id           STRING,
    amount               DOUBLE,
    currency             STRING,
    transaction_type     STRING,
    merchant_category    STRING,
    location             STRING,
    `timestamp`          BIGINT,
    is_international     BOOLEAN,
    event_time           TIMESTAMP,
    spark_proc_time      TIMESTAMP,
    source_to_kafka_lag_sec BIGINT,
    kafka_to_spark_lag_sec  BIGINT,
    total_latency_sec    BIGINT
)
PARTITIONED BY (dt DATE, hr INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/banking/bronze/transactions';
--2. The Silver Layer (Scored & Joined Alerts)
--Stores the results of your complex fraud logic and stream-stream joins
CREATE EXTERNAL TABLE IF NOT EXISTS silver_fraud_alerts (
    account_id           STRING,
    event_time           TIMESTAMP,
    transaction_id       STRING,
    amount               DOUBLE,
    risk_score           INT,
    alert_type           STRING,
    reason               STRING,
    tx_location          STRING,
    login_location       STRING,
    ip_address           STRING,
    device_type          STRING
)
PARTITIONED BY (dt DATE, hr INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/banking/silver/fraud_alerts';

--3. The DLQ Layer (SLA Violations)
--Stores late-arriving data that missed the 10-minute real-time window.
CREATE EXTERNAL TABLE IF NOT EXISTS dlq_transactions (
    transaction_id       STRING,
    account_id           STRING,
    amount               DOUBLE,
    total_latency_sec    BIGINT,
    event_time           TIMESTAMP
)
PARTITIONED BY (dt DATE, hr INT)
STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/user/hive/warehouse/banking/dlq/transactions';

-- Geographical Risk Summary
CREATE TABLE IF NOT EXISTS gold_location_risk (
    location             STRING,
    alert_count          INT,
    total_fraud_amount   DOUBLE,
    avg_risk_score       DOUBLE
) 
PARTITIONED BY (dt DATE)
STORED AS PARQUET;

-- Daily Executive KPIs
CREATE TABLE IF NOT EXISTS gold_daily_kpi_report (
    total_transactions   INT,
    total_alerts         INT,
    fraud_percentage     DOUBLE,
    high_risk_volume     DOUBLE
)
PARTITIONED BY (dt DATE)
STORED AS PARQUET;


-- Part 2: The Data Management Queries
--- 1. Syncing New Data (Airflow's most important task) --------
--- Whenever Spark writes new data, Hive needs to be told to refresh its list of folders:


MSCK REPAIR TABLE bronze_transactions;
MSCK REPAIR TABLE silver_fraud_alerts;
MSCK REPAIR TABLE dlq_transactions;