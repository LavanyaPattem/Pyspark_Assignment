import threading
import time
import logging
import pandas as pd
import uuid
from datetime import datetime
from pyspark.sql import SparkSession
from src.s3_manager import S3Manager
from src.database import DatabaseManager
from src.pattern_detector import PatternDetector
from config.config import Config
from src.gdrive_reader import GoogleDriveReader

class MechanismY:
    def __init__(self, spark_session: SparkSession):
        self.s3_manager = S3Manager(
            Config.S3_BUCKET,
            Config.AWS_REGION,
            Config.AWS_ACCESS_KEY_ID,
            Config.AWS_SECRET_ACCESS_KEY
        )
        self.db_manager = DatabaseManager(Config.POSTGRES_CONFIG)
        self.spark = spark_session
        self.pattern_detector = PatternDetector(self.spark, self.db_manager, GoogleDriveReader().customer_importance_df)
        self.running = False
        self.processed_files = set()
        self.detection_buffer = []
    
    def start(self):
        self.running = True
        thread = threading.Thread(target=self._run_detection)
        thread.daemon = True
        thread.start()
        logging.info("Mechanism Y started")
        return thread
    
    def stop(self):
        self.running = False
        self.spark.stop()
        logging.info("Mechanism Y stopped")
    
    def _run_detection(self):
        while self.running:
            try:
                unprocessed_chunks = self.db_manager.execute_query("""
                    SELECT chunk_id, s3_path FROM transaction_chunks 
                    WHERE processed = FALSE
                    ORDER BY created_at ASC
                """)
                
                if not unprocessed_chunks:
                    logging.info("No unprocessed chunks found")
                    if self.detection_buffer:
                        logging.info(f"Flushing {len(self.detection_buffer)} remaining detections")
                        self._flush_detections()
                    time.sleep(Config.PROCESSING_INTERVAL)
                    continue
                
                for chunk_id, s3_path in unprocessed_chunks:
                    chunk_df = self.s3_manager.read_csv_from_s3(s3_path)
                    
                    if chunk_df.empty:
                        logging.warning(f"Empty chunk: {chunk_id}, marking as processed")
                        self.db_manager.execute_query("""
                            UPDATE transaction_chunks SET processed = TRUE 
                            WHERE chunk_id = %s
                        """, (chunk_id,))
                        continue
                    
                    detections = self.pattern_detector.detect_patterns(chunk_df)
                    logging.info(f"Chunk {chunk_id}: Generated {len(detections)} detections")
                    
                    self.detection_buffer.extend(detections)
                    
                    self.db_manager.execute_query("""
                        UPDATE transaction_chunks SET processed = TRUE 
                        WHERE chunk_id = %s
                    """, (chunk_id,))
                    
                    logging.info(f"Processed chunk {chunk_id}, total detections in buffer: {len(self.detection_buffer)}")
                    
                    if self.detection_buffer:
                        logging.info(f"Flushing {len(self.detection_buffer)} detections")
                        self._flush_detections()
                
                time.sleep(Config.PROCESSING_INTERVAL)
                
            except Exception as e:
                logging.error(f"Error in Mechanism Y: {str(e)}")
                time.sleep(Config.PROCESSING_INTERVAL)
    
    def _flush_detections(self):
        if not self.detection_buffer:
            logging.info("No detections to flush")
            return
        
        try:
            batch = self.detection_buffer
            self.detection_buffer = []
            
            batch_df = pd.DataFrame(batch)
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            file_id = uuid.uuid4().hex[:8]
            s3_key = f"{Config.S3_OUTPUT_PREFIX}detections_{timestamp}_{file_id}.csv"
            
            if self.s3_manager.upload_dataframe(batch_df, s3_key):
                logging.info(f"Uploaded {len(batch)} detections to {s3_key}")
                
                for detection in batch:
                    self.db_manager.execute_query("""
                        INSERT INTO pattern_detections 
                        (detection_id, y_start_time, detection_time, pattern_id, 
                         action_type, customer_name, merchant_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """, (
                        uuid.uuid4().hex,
                        detection['y_start_time'],
                        detection['detection_time'],
                        detection['pattern_id'],
                        detection['action_type'],
                        detection['customer_name'],
                        detection['merchant_id']
                    ))
            else:
                logging.error(f"Failed to upload detections batch to {s3_key}")
                self.detection_buffer = batch + self.detection_buffer
        
        except Exception as e:
            logging.error(f"Error flushing detections: {str(e)}")
            self.detection_buffer = batch + self.detection_buffer