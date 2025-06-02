import threading
import time
import logging
import uuid
from datetime import datetime
from src.gdrive_reader import GoogleDriveReader
from src.s3_manager import S3Manager
from src.database import DatabaseManager
from config.config import Config

class MechanismX:
    def __init__(self):
        self.gdrive_reader = GoogleDriveReader()
        self.s3_manager = S3Manager(
            Config.S3_BUCKET,
            Config.AWS_REGION,
            Config.AWS_ACCESS_KEY_ID,
            Config.AWS_SECRET_ACCESS_KEY
        )
        self.db_manager = DatabaseManager(Config.POSTGRES_CONFIG)
        self.running = False
    
    def start(self):
        self.running = True
        thread = threading.Thread(target=self._run_ingestion)
        thread.daemon = True
        thread.start()
        logging.info("Mechanism X started")
        return thread
    
    def stop(self):
        self.running = False
        logging.info("Mechanism X stopped")
    
    def _run_ingestion(self):
        while self.running:
            try:
                chunk = self.gdrive_reader.get_next_chunk(Config.CHUNK_SIZE)
                
                if chunk.empty:
                    logging.info("No more data to process, stopping Mechanism X")
                    self.running = False
                    return
                
                chunk_id = f"chunk_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
                s3_key = f"{Config.S3_INPUT_PREFIX}{chunk_id}.csv"
                
                if self.s3_manager.upload_dataframe(chunk, s3_key):
                    self.db_manager.execute_query("""
                        INSERT INTO transaction_chunks (chunk_id, s3_path, processed)
                        VALUES (%s, %s, %s)
                    """, (chunk_id, s3_key, False))
                    logging.info(f"Uploaded chunk {chunk_id} with {len(chunk)} transactions to {s3_key}")
                else:
                    logging.error(f"Failed to upload chunk {chunk_id} to {s3_key}, skipping database insertion")
                
                time.sleep(Config.PROCESSING_INTERVAL)
                
            except Exception as e:
                logging.error(f"Error in Mechanism X: {str(e)}")
                time.sleep(Config.PROCESSING_INTERVAL)