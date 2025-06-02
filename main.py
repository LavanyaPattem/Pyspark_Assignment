import time
import logging
from pyspark.sql import SparkSession
from src.x import MechanismX
from src.y import MechanismY

class TransactionProcessingSystem:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.mechanism_x = None
        self.mechanism_y = None
        self.setup_logging()
    
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler()
            ]
        )
    
    def start(self):
        logging.info("Starting Transaction Processing System")
        
        self.mechanism_x = MechanismX()
        self.mechanism_y = MechanismY(self.spark)
        
        x_thread = self.mechanism_x.start()
        y_thread = self.mechanism_y.start()
        
        return x_thread, y_thread
    
    def stop(self):
        logging.info("Stopping Transaction Processing System")
        
        if self.mechanism_x:
            self.mechanism_x.stop()
        
        if self.mechanism_y:
            self.mechanism_y.stop()