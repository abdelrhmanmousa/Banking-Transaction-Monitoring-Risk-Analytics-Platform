from pyspark.sql import SparkSession
import sys

def compact_layer(date, hour):
    spark = (
        SparkSession.builder
        .appName(f"Compactor_Hour_{hour}")
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse")
        
        .getOrCreate()
    )
    
    # Paths from your Config logic
    bronze_path = "hdfs://namenode:9000/user/hive/warehouse/banking/bronze/transactions/dt={}/hr={}".format(date, hour)
    silver_path = "hdfs://namenode:9000/user/hive/warehouse/banking/silver/transactions/dt={}/hr={}".format(date, hour)

    # 1. Read the hundreds of tiny files from Bronze
    print("Reading Bronze data for {}:{}".format(date, hour))
    df = spark.read.parquet(bronze_path)

    # 2. Merge into 1 single high-performance file
    # 3. Overwrite into Silver
    df.coalesce(1).write.mode("overwrite").parquet(silver_path)
    
    print("Compaction successful. Silver layer optimized.")
    spark.stop()

if __name__ == "__main__":
    compact_layer(sys.argv[1], sys.argv[2])