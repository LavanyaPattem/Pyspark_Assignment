from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import logging
from datetime import datetime
from typing import List, Dict
from src.database import DatabaseManager
from config.config import Config

class PatternDetector:
    def __init__(self, spark_session: SparkSession, db_manager: DatabaseManager, customer_importance_df: pd.DataFrame):
        self.spark = spark_session
        self.db = db_manager
        self.y_start_time = datetime.now(Config.IST)
        
        expected_importance_cols = {'Source', 'Target', 'Weight', 'typeTrans', 'fraud'}
        if not expected_importance_cols.issubset(set(customer_importance_df.columns)):
            raise ValueError(f"Customer importance data missing required columns: {expected_importance_cols - set(customer_importance_df.columns)}")
        
        schema = StructType([
            StructField("Source", StringType(), True),
            StructField("Target", StringType(), True),
            StructField("Weight", DoubleType(), True),
            StructField("typeTrans", StringType(), True),
            StructField("fraud", LongType(), True)
        ])
        
        self.customer_importance_df = spark_session.createDataFrame(customer_importance_df, schema=schema)
        logging.info("Initialized customer_importance_df with schema validation")
    
    def detect_patterns(self, transactions_df: pd.DataFrame) -> List[Dict]:
        try:
            expected_cols = {'step', 'customer', 'age', 'gender', 'zipcodeOri', 'merchant', 'zipMerchant', 'category', 'amount', 'fraud'}
            if not expected_cols.issubset(set(transactions_df.columns)):
                logging.error(f"Invalid transaction data schema: missing {expected_cols - set(transactions_df.columns)}")
                return []
            
            spark_df = self.spark.createDataFrame(transactions_df).cache()
            detections = []
            
            detections.extend(self.detect_pattern_1(spark_df))
            detections.extend(self.detect_pattern_2(spark_df))
            detections.extend(self.detect_pattern_3(spark_df))
            
            spark_df.unpersist()
            return detections
        except Exception as e:
            logging.error(f"Error in pattern detection: {str(e)}")
            return []
    
    def detect_pattern_1(self, spark_df) -> List[Dict]:
        detections = []
        
        try:
            merchant_counts = spark_df.groupBy("merchant").count().collect()
            logging.info(f"Pattern 1: Found {len(merchant_counts)} merchants with transactions")
            
            if merchant_counts:
                cursor = self.db.conn.cursor()
                try:
                    for row in merchant_counts:
                        merchant_id = row["merchant"]
                        count = row["count"]
                        cursor.execute("""
                            INSERT INTO merchant_stats (merchant_id, total_transactions, last_updated)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (merchant_id) 
                            DO UPDATE SET 
                                total_transactions = merchant_stats.total_transactions + EXCLUDED.total_transactions,
                                last_updated = EXCLUDED.last_updated
                        """, (merchant_id, count, datetime.now()))
                    self.db.conn.commit()
                finally:
                    cursor.close()
            
            eligible_merchants = self.db.execute_query("""
                SELECT merchant_id FROM merchant_stats WHERE total_transactions > 10000
            """)
            logging.info(f"Pattern 1: Found {len(eligible_merchants)} eligible merchants (>10K transactions)")
            
            if not eligible_merchants:
                return detections
            
            eligible_merchant_ids = [row[0] for row in eligible_merchants]
            
            for merchant_id in eligible_merchant_ids:
                merchant_transactions = spark_df.filter(col("merchant") == merchant_id)
                
                if merchant_transactions.count() == 0:
                    continue
                
                customer_counts = merchant_transactions.groupBy("customer").count()
                
                total_customers = customer_counts.count()
                top_1_percent_count = max(1, int(total_customers * 0.01))
                
                top_customers = customer_counts.orderBy(col("count").desc()).limit(top_1_percent_count)
                logging.info(f"Pattern 1: Merchant {merchant_id} has {total_customers} customers, top 1% = {top_1_percent_count}")
                
                top_customer_ids = [row["customer"] for row in top_customers.collect()]
                if not top_customer_ids:
                    logging.info(f"Pattern 1: No top customers found for merchant {merchant_id}")
                    continue
                
                weights = self.customer_importance_df.filter(col("Source").isin(top_customer_ids))
                total_weights = weights.count()
                if total_weights == 0:
                    logging.info(f"Pattern 1: No weights found for top customers of merchant {merchant_id}")
                    continue
                
                bottom_1_percent_count = max(1, int(total_weights * 0.01))
                bottom_weights = weights.orderBy(col("Weight").asc()).limit(bottom_1_percent_count)
                logging.info(f"Pattern 1: Found {total_weights} weights, bottom 1% = {bottom_1_percent_count}")
                
                bottom_weights_rows = bottom_weights.collect()
                if not bottom_weights_rows:
                    logging.info(f"Pattern 1: No bottom weights found for merchant {merchant_id}")
                    continue
                
                for customer_row in bottom_weights_rows:
                    customer_name = customer_row["Source"]
                    if not customer_name:
                        logging.warning(f"Pattern 1: Missing customer_name in weights for merchant {merchant_id}")
                        continue
                    
                    detection = {
                        "y_start_time": self.y_start_time,
                        "detection_time": datetime.now(Config.IST),
                        "pattern_id": "PatId1",
                        "action_type": "UPGRADE",
                        "customer_name": customer_name,
                        "merchant_id": merchant_id
                    }
                    detections.append(detection)
        
        except Exception as e:
            logging.error(f"Error in pattern 1 detection: {str(e)}")
        
        logging.info(f"Pattern 1: Detected {len(detections)} UPGRADE patterns")
        return detections
    
    def detect_pattern_2(self, spark_df) -> List[Dict]:
        detections = []
        
        count = 0
        try:
            customer_stats = spark_df.groupBy("customer", "merchant").agg(
                count("*").alias("transaction_count"),
                avg("amount").alias("avg_amount")
            )
            
            pattern_customers = customer_stats.filter(
                (col("avg_amount") < lit(30)) & (col("transaction_count") >= lit(50))
            )
            count = pattern_customers.count()
            logging.info(f"Pattern 2: Found {count} customers with avg_amount < 30 and transaction_count >= 50")
            
            for row in pattern_customers.collect():
                detection = {
                    "y_start_time": self.y_start_time,
                    "detection_time": datetime.now(Config.IST),
                    "pattern_id": "PatId2",
                    "action_type": "CHILD",
                    "customer_name": row["customer"],
                    "merchant_id": row["merchant"]
                }
                detections.append(detection)
        
        except Exception as e:
            logging.error(f"Error in pattern 2 detection: {str(e)}")
        
        logging.info(f"Pattern 2: Detected {len(detections)} CHILD patterns")
        return detections
    
    def detect_pattern_3(self, spark_df) -> List[Dict]:
        detections = []
        
        try:
            gender_stats = spark_df.groupBy("merchant").agg(
                countDistinct(when(col("gender") == "M", col("customer"))).alias("male_count"),
                countDistinct(when(col("gender") == "F", col("customer"))).alias("female_count")
            )
            
            pattern_merchants = gender_stats.filter(
                (col("female_count") < col("male_count")) & (col("female_count") > lit(100))
            )
            
            count = pattern_merchants.count()
            logging.info(f"Pattern 3: Found {count} merchants with female_count < male_count and female_count > 100")
            
            for row in pattern_merchants.collect():
                detection = {
                    "y_start_time": self.y_start_time,
                    "detection_time": datetime.now(Config.IST),
                    "pattern_id": "PatId3",
                    "action_type": "DEI-NEEDED",
                    "customer_name": "",
                    "merchant_id": row["merchant"]
                }
                detections.append(detection)
        
        except Exception as e:
            logging.error(f"Error in pattern 3 detection: {str(e)}")
        
        logging.info(f"Pattern 3: Detected {len(detections)} DEI-NEEDED patterns")
        return detections