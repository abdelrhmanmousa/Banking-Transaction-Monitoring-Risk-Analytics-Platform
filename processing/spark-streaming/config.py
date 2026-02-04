# config.py
from dataclasses import dataclass

@dataclass(frozen=True)
class AppConfig:
    # Kafka / Schema Registry
    KAFKA_BOOTSTRAP: str = "broker:29092"
    SCHEMA_REGISTRY: str = "http://schema-registry:8081"

    # Topics
    TOPIC_TX: str = "banking_transactions"
    TOPIC_LOGINS: str = "user_logins"
    TOPIC_ACTIVITY: str = "account_activity"

    # HDFS base (lakehouse style)
    HDFS_BASE: str = "hdfs://namenode:9000/user/hive/warehouse/banking"

    # BRONZE (raw decoded , no joins/scoring)
    BRONZE_TX_PATH: str = f"{HDFS_BASE}/bronze/transactions"
    BRONZE_LOGINS_PATH: str = f"{HDFS_BASE}/bronze/logins"
    BRONZE_ACTIVITY_PATH: str = f"{HDFS_BASE}/bronze/activity"

    # SILVER (joined + scored)
    SILVER_TX_PATH: str = f"{HDFS_BASE}/silver/transactions"
    ALERTS_PATH: str = f"{HDFS_BASE}/silver/fraud_alerts"
    SILVER_AC_PATH: str = f"{HDFS_BASE}/silver/activity"
    SILVER_LG_PATH: str = f"{HDFS_BASE}/silver/logins"

    # DLQ Late events
    DLQ_BASE_PATH: str = f"{HDFS_BASE}/dlq"
    DLQ_TX_PATH: str = f"{DLQ_BASE_PATH}/transactions"
    DLQ_LOGINS_PATH: str = f"{DLQ_BASE_PATH}/user_logins"
    DLQ_ACTIVITY_PATH: str = f"{DLQ_BASE_PATH}/account_activity"
    DLQ_ALERTS_PATH: str = f"{DLQ_BASE_PATH}/fraud_alerts"
    # Checkpoints
    CHECKPOINT_BASE: str = "hdfs://namenode:9000/checkpoints/banking"
    CHECKPOINT_BRONZE_TX: str = f"{CHECKPOINT_BASE}/bronze_tx"
    CHECKPOINT_BRONZE_LOGINS: str = f"{CHECKPOINT_BASE}/bronze_logins"
    CHECKPOINT_BRONZE_ACTIVITY: str = f"{CHECKPOINT_BASE}/bronze_activity"
    CHECKPOINT_SILVER_TX: str = f"{CHECKPOINT_BASE}/fraud_detection_engine_v2"


    # Spark tuning (WSL 7GB friendly)
    SHUFFLE_PARTITIONS: int = 12
    MAX_OFFSETS_PER_TRIGGER: int = 3000
    TRIGGER_INTERVAL: str = "5 seconds"

    # Stream joins
    WATERMARK_DELAY: str = "10 minutes" 
    LATE_ALLOWANCE: str = "10 minutes"
    LOGIN_TX_WINDOW: str = "1 minute" 
    ACTIVITY_TX_WINDOW: str = "5 minutes" 

    # Risk scoring
    HIGH_RISK_THRESHOLD: int = 50

    # HBase thrift
    HBASE_PORT: int = 9090
    HBASE_TABLE: str = "banking_risk"
    HBASE_HOST: str = "hbase-thrift"
