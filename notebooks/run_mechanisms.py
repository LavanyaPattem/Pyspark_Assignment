# Databricks notebook source
from pyspark.sql import SparkSession
from src.main import TransactionProcessingSystem

# COMMAND ----------

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("TransactionProcessingSystem") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .getOrCreate()

# COMMAND ----------

# Start the Transaction Processing System
system = TransactionProcessingSystem(spark)

try:
    x_thread, y_thread = system.start()
    
    # Keep the notebook running until all chunks are processed
    while True:
        dbutils.notebook.run("sleep", 10)  # Sleep for 10 seconds
        unprocessed_chunks = system.mechanism_y.db_manager.execute_query("""
            SELECT COUNT(*) FROM transaction_chunks WHERE processed = FALSE
        """)[0][0]
        if unprocessed_chunks == 0 and not system.mechanism_x.running:
            print("All chunks processed and Mechanism X stopped, shutting down")
            break
        print("System running...")

except Exception as e:
    print(f"System error: {str(e)}")
    system.stop()

# COMMAND ----------

# Stop the system
system.stop()
spark.stop()