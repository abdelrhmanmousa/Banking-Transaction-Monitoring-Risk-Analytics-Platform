# sinks.py
from pyspark.sql import DataFrame
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import col
import happybase


def write_parquet_partitioned(df: DataFrame, path: str):
    """ Saves the DataFrame as partitioned Parquet files by 'dt' and 'hr' columns. """
    # coalesce(1) is used here to reduce the number of small files within the micro-batches.
    df.coalesce(1).write.partitionBy("dt", "hr").mode("append").parquet(path)


def write_alerts_to_hbase(alerts_df: DataFrame, host: str, port: int, table_name: str):
    """ speed layer: writes alert records to HBase via happybase (thrift). """
    def send_partition(rows):
        conn = None
        try:
            conn = happybase.Connection(host, port=port)
            conn.open()
            table = conn.table(table_name)

            with table.batch(batch_size=200) as b:
                for r in rows:
                    # Row key : account_id + timestamp_ms + tx_id (Guarantees uniqueness)
                    # we use.formate for spark 3.0 compatibility
                    ts_ms = int(r.event_time.timestamp() * 1000)
                    rk = f"{r.account_id}_{ts_ms}_{r.transaction_id}".encode()

                    b.put(rk, {
                        b"al:type": str(r.alert_type).encode(),
                        b"al:reason": str(r.reason).encode(),
                        b"tx:id": str(r.transaction_id).encode(),
                        b"tx:amount": str(r.amount).encode(),
                        b"tx:risk": str(r.risk_score).encode(),
                    })
        except Exception as e:  
            print(f"Error writing to HBase: {e}")
                    
        finally:
            if conn:
                conn.close()

    alerts_df.foreachPartition(send_partition)


def foreach_batch_router(cfg , write_hbase: bool = True):
    """
    Modular Router that implements Medallion Architecture + DLQ.
    Routes records based on the '_type' column added in processor.py.
    """
    def _sink(batch_df: DataFrame, batch_id: int):
        # spark empty dataframe check
        if batch_df.rdd.isEmpty():
            return
        # persist to avoid re-caculating the joins for different HDFS Paths
        batch_df.persist(StorageLevel.MEMORY_AND_DISK)

        # ROUTE 1 : BRONZE (The Audit Trial - All on-time transactions) )
        bronze_tx = batch_df.filter(col("_type") == "BRONZE_TX").drop("_type")
        bronze_login = batch_df.filter(col("_type") == "BRONZE_LOGIN").drop("_type")
        bronze_activity = batch_df.filter(col("_type") == "BRONZE_ACTIVITY").drop("_type")
        
        if not bronze_tx.rdd.isEmpty():
            write_parquet_partitioned(bronze_tx, cfg.BRONZE_TX_PATH)
        if not bronze_login.rdd.isEmpty():
            write_parquet_partitioned(bronze_login, cfg.BRONZE_LOGINS_PATH)
        if not bronze_activity.rdd.isEmpty():
            write_parquet_partitioned(bronze_activity, cfg.BRONZE_ACTIVITY_PATH)
        
        # ROUTE 2 : SILVER (Cleaned Transactions - scored/on-time transactions only)    
        silver_df = batch_df.filter(col("_type") == "SILVER_TX").drop("_type")
        
        if not silver_df.rdd.isEmpty():
            write_parquet_partitioned(silver_df, cfg.SILVER_TX_PATH)
            
        # ROUTE 3 : ALERTS (HDFS + HBase)
        al_df = batch_df.filter(col("_type") == "ALERT").drop("_type")
        if not al_df.rdd.isEmpty():
            write_parquet_partitioned(al_df, cfg.ALERTS_PATH)
            if write_hbase:
                write_alerts_to_hbase(al_df, cfg.HBASE_HOST, cfg.HBASE_PORT, cfg.HBASE_TABLE    )     

        # ROUTE 4 : LATE EVENTS (DLQ)
        late_tx = batch_df.filter(col("_type") == "LATE_TX").drop("_type")
        late_login = batch_df.filter(col("_type") == "LATE_LOGIN").drop("_type")
        late_activity = batch_df.filter(col("_type") == "LATE_ACTIVITY").drop("_type")

        if not late_tx.rdd.isEmpty():
            write_parquet_partitioned(late_tx, f"{cfg.DLQ_BASE_PATH}/transactions")
        if not late_login.rdd.isEmpty():
            write_parquet_partitioned(late_login, f"{cfg.DLQ_BASE_PATH}/logins")
        if not late_activity.rdd.isEmpty():
            write_parquet_partitioned(late_activity, f"{cfg.DLQ_BASE_PATH}/activity")

        batch_df.unpersist()

    return _sink
