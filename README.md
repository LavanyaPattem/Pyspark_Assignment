This project implements a transaction processing system using Databricks, AWS S3, and AWS RDS PostgreSQL. It ingests data from Google Drive, processes it, and detects patterns using PySpark.
Project Structure
transaction-processing-system/
│
├── config/
│   └── config.py           # Configuration settings for Databricks, AWS, and RDS
│
├── src/
│   ├── __init__.py         # Makes src a package
│   ├── database.py         # DatabaseManager for AWS RDS PostgreSQL
│   ├── s3_manager.py       # S3Manager for AWS S3 operations
│   ├── gdrive_reader.py    # GoogleDriveReader for data ingestion
│   ├── pattern_detector.py # PatternDetector for pattern detection
│   ├── x.py                # MechanismX for data ingestion
│   ├── y.py                # MechanismY for pattern detection
│   └── main.py             # Entry point of the application
│
├── datasets/               # Placeholder for datasets
│
├── env/
│   └── .env.example        # Example environment file
│
├── notebooks/
│   └── run_mechanisms.py   # Databricks notebook to run the system
│
├── README.md               # Project documentation
│
└── requirements.txt        # Dependencies list for Databricks

Setup Instructions
1. Clone the Repository
git clone <your-repo-url>
cd transaction-processing-system

2. Set Up Databricks Workspace

Create a new Databricks workspace or use an existing one.
Upload the repository to your Databricks workspace:
In Databricks, go to Repos > Add Repo > Upload.
Upload the entire transaction-processing-system folder.



3. Install Dependencies in Databricks

Create a Databricks cluster or use an existing one.
Install the required libraries on the cluster:
Go to Clusters > Select your cluster > Libraries > Install New.
Choose PyPI and install each package listed in requirements.txt:
pandas==2.2.2
boto3==1.34.117
psycopg2-binary==2.9.9
pyspark==3.5.1
pytz==2024.1





4. Configure AWS Credentials

Copy env/.env.example to env/.env and update with your AWS credentials:cp env/.env.example env/.env

Edit env/.env:AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your_secret_key
AWS_DEFAULT_REGION=us-east-1


In Databricks, set the environment variables:
Go to Clusters > Select your cluster > Advanced Options > Spark > Environment Variables.
Add the variables from env/.env.



5. Verify AWS RDS PostgreSQL Setup

Ensure your AWS RDS PostgreSQL instance is running (pyspark-db.c1abc2defghi.us-east-1.rds.amazonaws.com).
Verify the database pyspark-db exists and is accessible with user postgres and password Lavanyapattem.
If the tables don’t exist, they will be created automatically by database.py when the script runs.

6. Update Google Drive File IDs
In config/config.py, update the following with your Google Drive file IDs:
TRANSACTIONS_FILE_ID = 'your_transactions_csv_file_id'
CUSTOMER_IMPORTANCE_FILE_ID = 'your_customer_importance_csv_file_id'

7. Run the Application in Databricks

Open the notebooks/run_mechanisms.py notebook in Databricks.
Attach the notebook to your cluster.
Run all cells in the notebook to start Mechanism X and Mechanism Y.
Monitor the output in the notebook and check the S3 bucket for results (s3://pyspark-assignment-lavanya/pattern-detections/).

Assumptions

Transaction Data Schema:

Transactions: step, customer, age, gender, zipcodeOri, merchant, zipMerchant, category, amount, fraud
Customer Importance: Source, Target, Weight, typeTrans, fraud


Google Drive Access:

Files are public or accessible via direct links.
Sample data is generated if Google Drive fails.


AWS Configuration:

S3 bucket: pyspark-assignment-lavanya.
Region: us-east-1.
Required IAM permissions: s3:ListBucket, s3:GetObject, s3:PutObject, s3:CreateBucket.


AWS RDS PostgreSQL:

Endpoint: db.c1abhjugi.us-east-1.rds.amazonaws.com.
Database: postgres.
User: postgres, Password: password.


Pattern Detection:

Pattern 1: Top 1% customers with bottom 1% weights.
Pattern 2: Customers with avg amount < 30, >= 50 transactions.
Pattern 3: Merchants with female customers < male customers and female customers > 100.


Performance:

Single-node Spark setup in Databricks.
Processing interval: 2 seconds.



Troubleshooting

S3 Access Denied (403 Forbidden):

Verify AWS credentials in env/.env and ensure proper IAM permissions.
Check S3 bucket policies for pyspark-assignment-lavanya.


RDS Connection Issues:

Ensure the RDS instance is publicly accessible or within the same VPC as Databricks.
Check security group rules to allow inbound traffic on port 5432.
Verify the credentials in config/config.py.


Google Drive Access Failed:

Ensure file IDs are correct and files are accessible.
The script will fall back to sample data if Google Drive fails.


Databricks Dependencies:

If you encounter boto3 or botocore errors, ensure the versions match requirements.txt.
Restart the cluster after installing libraries.



Logs
Logs are output to the Databricks notebook console. You can also configure additional logging to a file in DBFS if needed.
