from airflow import DAG
from airflow.providers.apache.hdfs.sensors.web_hdfs import WebHdfsSensor
from airflow.operators.bash import BashOperator
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'mousa',
    'start_date': datetime(2026, 2, 3),
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG('banking_data_lake_maintenance',
         default_args=default_args,
         schedule_interval='@hourly', # Runs once per hour
         catchup=False) as dag:

    # 1. Wait for Spark Streaming to create the hour's folder
    # Logic: It looks for the folder for the PREVIOUS hour
    wait_for_bronze = WebHdfsSensor(
        task_id='sense_bronze_data',
        filepath='/user/hive/warehouse/banking/bronze/transactions/dt={{ data_interval_start.strftime("%Y-%m-%d") }}/hr={{ data_interval_start.strftime("%H") }}',
        webhdfs_conn_id='webhdfs_default',
        poke_interval=60 ,
        timeout = 600,
        mode = 'poke'
    )

    # 2. Sync the Bronze Hive Table
    sync_bronze = BashOperator(
        task_id='repair_bronze_metadata',
        bash_command =  ('set -e; '
        'for i in 1 2 3 4 5 6; do '
        '  echo "Waiting for HiveServer2... attempt $i"; '
        '  beeline -u "jdbc:hive2://hive-server:10000/default" -e "SHOW DATABASES;" && break || true; '
        '  sleep 10; '
        'done; '
        'beeline -u "jdbc:hive2://hive-server:10000/default" -e "'
        "MSCK REPAIR TABLE default.bronze_transactions;"
        '"'
    ),
        dag=dag
        
    )

    # 3. Run the Compaction Job (Bronze -> Silver)
    # This turns 700 files into 1 file
    compact_to_silver = SparkSubmitOperator( 
                        task_id="compact_to_silver", 
                        conn_id="spark_default",
                        application="/opt/airflow/batch/compactor.py",
                        application_args=[
        "{{ data_interval_start.strftime('%Y-%m-%d') }}", 
        "{{ data_interval_start.strftime('%H') }}"
    ],
                         conf={
        "spark.driver.memory": "512m",    # Reduce Driver memory
        "spark.executor.memory": "512m",  # Reduce Executor memory
        "spark.cores.max": "1",           # Only use 1 Core
        "spark.network.timeout": "300s",  # Increase timeout to 5 minutes
        "spark.executor.heartbeatInterval": "60s",
        "spark.driver.bindAddress": "0.0.0.0",
        "spark.driver.host": "airflow",   
        "spark.driver.port": "32000",
        "spark.blockManager.port": "32001",
    }, 
                    
                        verbose=True )

    # 4. Sync the Silver Hive Table
    sync_silver = BashOperator(
        task_id='repair_silver_metadata',
        bash_command=(
            'beeline -u "jdbc:hive2://hive-server:10000/default" '
            '-e "MSCK REPAIR TABLE default.silver_fraud_alerts;"'
        ),
        
    )
    
    load_location_gold = BashOperator(
    task_id="load_gold_location_risk",
    bash_command=(
        'beeline -u "jdbc:hive2://hive-server:10000/default" '
        '-e "'
        "INSERT OVERWRITE TABLE default.gold_location_risk PARTITION (dt=DATE '{{ ds }}') "
        "SELECT tx_location, COUNT(*), SUM(amount), AVG(risk_score) "
        "FROM default.silver_fraud_alerts "
        "WHERE dt = DATE '{{ ds }}' "
        "GROUP BY tx_location"
        '"'
    ),
)

    # 6. Load the KPI Gold Table
    load_kpi_gold = BashOperator(
    task_id="load_gold_daily_kpis",
    bash_command=(
        'beeline -u "jdbc:hive2://hive-server:10000/default" '
        '-e "'
        "INSERT OVERWRITE TABLE default.gold_daily_kpi_report PARTITION (dt=DATE '{{ ds }}') "
        "SELECT "
        "  COUNT(*), "
        "  SUM(CAST(is_high_risk AS INT)), "
        "  (SUM(CAST(is_high_risk AS INT)) / COUNT(*)) * 100, "
        "  SUM(CASE WHEN is_high_risk = true THEN amount ELSE 0 END) "
        "FROM transactions "
        "WHERE dt = DATE '{{ ds }}'"
        '"'
    ),
)


    # Flow: Sensor -> Sync Bronze -> Compact -> Sync Silver
    wait_for_bronze >> sync_bronze >> compact_to_silver >> sync_silver >> [load_location_gold, load_kpi_gold]