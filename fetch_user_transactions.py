import boto3
import pandas as pd
from io import StringIO

def get_user_transactions(user_id):
    """
    Get user transactions from S3 CSV file
    
    Args:
        user_id (str): User ID to filter transactions
        
    Returns:
        pd.DataFrame: DataFrame containing user transactions
    """
    try:
        s3_client = boto3.client('s3')
        bucket_name = "notifi-transaction-dataset"
        key = "notifi-dump/transaction_data_final.csv"
        
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        
        # Filter to the specific user
        user_df = df[df['User_id'] == user_id]
        
        # Check for empty result
        if user_df.empty:
            print(f"Warning: No transactions found for user {user_id}")
            return pd.DataFrame()
            
        # Convert date and check month distribution
        user_df['Txn Date'] = pd.to_datetime(user_df['Txn Date'], errors='coerce')
        
        # Count transactions by month
        month_counts = user_df.groupby(user_df['Txn Date'].dt.strftime('%Y-%m')).size()
        print(f"Found {len(month_counts)} months of data for user {user_id}:")
        for month, count in sorted(month_counts.items()):
            print(f"  {month}: {count} transactions")
            
        return user_df
    except Exception as e:
        print(f"Error reading transactions for user {user_id} from S3: {e}")
        return pd.DataFrame()  # Return empty DataFrame on error