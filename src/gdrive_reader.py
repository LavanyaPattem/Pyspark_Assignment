import pandas as pd
import logging
import random
import string
from config.config import Config
import builtins

class GoogleDriveReader:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(GoogleDriveReader, cls).__new__(cls)
            cls._instance.transactions_df = None
            cls._instance.customer_importance_df = None
            cls._instance.current_row = 0
            cls._instance.load_data()
        return cls._instance
    
    def load_data(self):
        try:
            transactions_url = f"https://drive.google.com/uc?export=download&id={Config.TRANSACTIONS_FILE_ID}"
            customer_importance_url = f"https://drive.google.com/uc?export=download&id={Config.CUSTOMER_IMPORTANCE_FILE_ID}"
            
            self.transactions_df = pd.read_csv(transactions_url)
            self.customer_importance_df = pd.read_csv(customer_importance_url)
            
            expected_transaction_cols = {'step', 'customer', 'age', 'gender', 'zipcodeOri', 'merchant', 'zipMerchant', 'category', 'amount', 'fraud'}
            expected_importance_cols = {'Source', 'Target', 'Weight', 'typeTrans', 'fraud'}
            if not expected_transaction_cols.issubset(set(self.transactions_df.columns)):
                raise ValueError(f"Transaction data missing required columns: {expected_transaction_cols - set(self.transactions_df.columns)}")
            if not expected_importance_cols.issubset(set(self.customer_importance_df.columns)):
                raise ValueError(f"Customer importance data missing required columns: {expected_importance_cols - set(self.customer_importance_df.columns)}")
            
            logging.info(f"Loaded {len(self.transactions_df)} transactions")
            logging.info(f"Loaded {len(self.customer_importance_df)} customer importance records")
            
        except Exception as e:
            logging.warning(f"Failed to load data from Google Drive: {str(e)}. Falling back to sample data.")
            self.create_sample_data()
    
    def create_sample_data(self):
        customers = [f"C_{''.join(random.choices(string.ascii_uppercase + string.digits, k=8))}" for _ in range(2000)]
        merchants = [f"M_{''.join(random.choices(string.ascii_uppercase + string.digits, k=8))}" for _ in range(100)]
        categories = ['es_transportation', 'es_health', 'es_food', 'es_shopping']
        transaction_types = ['Purchase', 'Withdrawal', 'Transfer', 'Payment']
        
        transactions = []
        for i in range(600000):
            merchant = random.choice(merchants[:10]) if i < 50000 else random.choice(merchants)
            customer = random.choice(customers[:50]) if i < 10000 else random.choice(customers)
            amount = random.uniform(1, 20) if i < 20000 else random.uniform(1, 1000)
            transactions.append({
                'step': i % 720,
                'customer': customer,
                'age': random.randint(18, 80),
                'gender': 'M' if i < 15000 else random.choice(['M', 'F']),
                'zipcodeOri': f"'{random.randint(10000, 99999)}'",
                'merchant': merchant,
                'zipMerchant': f"'{random.randint(10000, 99999)}'",
                'category': random.choice(categories),
                'amount': round(amount, 2),
                'fraud': random.randint(0, 1)
            })
        
        self.transactions_df = pd.DataFrame(transactions)
        
        importance_data = []
        for _ in range(600000):
            customer = random.choice(customers[:50]) if random.random() < 0.2 else random.choice(customers)
            importance_data.append({
                'Source': customer,
                'Target': random.choice(merchants[:10]) if random.random() < 0.3 else random.choice(merchants),
                'Weight': round(random.uniform(0.1, 0.3) if _ < 1000 else random.uniform(0.1, 1.0), 2),
                'typeTrans': random.choice(transaction_types),
                'fraud': random.randint(0, 1)
            })
        
        self.customer_importance_df = pd.DataFrame(importance_data)
        logging.info("Created sample data for demonstration")
    
    def get_next_chunk(self, chunk_size: int) -> pd.DataFrame:
        try:
            if self.transactions_df is None or self.transactions_df.empty:
                logging.warning("No transaction data available")
                return pd.DataFrame()
            
            if self.current_row >= len(self.transactions_df):
                logging.info("No more data to process")
                return pd.DataFrame()
            
            end_row = builtins.min(self.current_row + chunk_size, len(self.transactions_df))
            chunk = self.transactions_df.iloc[self.current_row:end_row].copy()
            self.current_row = end_row
            
            return chunk
        except Exception as e:
            logging.error(f"Error getting next chunk: {str(e)}")
            return pd.DataFrame()