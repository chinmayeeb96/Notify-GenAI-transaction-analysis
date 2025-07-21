import boto3
import pandas as pd
from io import StringIO

def get_user_ids():
    """
    Get user IDs from S3 CSV file
    """
    try:
        s3_client = boto3.client('s3')
        bucket_name = "notifi-transaction-dataset"
        key = "notifi-dump/user.csv"
        
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        
        return df['User_id'].unique().tolist()
    except Exception as e:
        print(f"Error reading user IDs from S3: {e}")
        return []