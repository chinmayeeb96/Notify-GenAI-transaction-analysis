import boto3
import pandas as pd
from io import StringIO

def get_user_transactions(user_id):
    """
    Get user transactions from S3 CSV file
    """
    try:
        s3_client = boto3.client('s3')
        bucket_name = "notifi-transaction-dataset"
        key = "notifi-dump/transaction_data_final.csv"
        
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        
        return df[df['User_id'] == user_id]
    except Exception as e:
        print(f"Error reading transactions for user {user_id} from S3: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error