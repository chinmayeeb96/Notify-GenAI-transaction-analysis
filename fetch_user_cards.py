import boto3
import pandas as pd
from io import StringIO

def get_user_cards(user_id):
    """
    Get user card IDs from S3 CSV file
    """
    try:
        s3_client = boto3.client('s3')
        bucket_name = "notifi-transaction-dataset"
        key = "notifi-dump/user_card.csv"
        
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        
        return df[df['User_id'] == user_id]['Card_id'].tolist()
    except Exception as e:
        print(f"Error reading user cards for {user_id} from S3: {e}")
        return []
