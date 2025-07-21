import pandas as pd
import boto3
import json
from io import StringIO

def update_s3_transaction_data():
    """
    Upload the fixed transaction data to S3
    """
    try:
        # Read the fixed data
        fixed_data_path = "/Users/chinmayee.dalai/Documents/genai_expo/data/data_generation/transaction_data/transaction_data_final.csv"
        df = pd.read_csv(fixed_data_path)
        
        # S3 configuration
        s3_client = boto3.client('s3')
        bucket_name = "notifi-transaction-dataset"
        key = "notifi-dump/transaction_data_final.csv"
        
        # Convert DataFrame to CSV
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=csv_buffer.getvalue()
        )
        
        print(f"Successfully updated transaction data in S3 bucket: s3://{bucket_name}/{key}")
        
    except Exception as e:
        print(f"Error updating transaction data in S3: {e}")

if __name__ == "__main__":
    # Ask for confirmation before uploading
    confirm = input("This will update the transaction data in S3. Are you sure? (y/n): ")
    if confirm.lower() == 'y':
        update_s3_transaction_data()
    else:
        print("Operation cancelled.")
