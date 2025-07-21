import boto3
import pandas as pd
from io import StringIO

def analyze_transaction_data():
    """
    Analyze the transaction data to understand month distribution by user
    """
    try:
        # Read transaction data from S3
        s3_client = boto3.client('s3')
        bucket_name = "notifi-transaction-dataset"
        key = "notifi-dump/transaction_data_final.csv"
        
        print(f"Reading transaction data from s3://{bucket_name}/{key}...")
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(StringIO(csv_content))
        
        print(f"Total transactions: {len(df)}")
        
        # Convert date column to datetime
        df['Txn Date'] = pd.to_datetime(df['Txn Date'], errors='coerce')
        
        # Count invalid dates
        invalid_dates = df['Txn Date'].isna().sum()
        if invalid_dates > 0:
            print(f"Warning: {invalid_dates} transactions with invalid dates")
            df = df.dropna(subset=['Txn Date'])
        
        # Add month column for analysis
        df['month'] = df['Txn Date'].dt.strftime('%Y-%m')
        
        # Overall month distribution
        print("\nOverall month distribution:")
        month_counts = df.groupby('month').size().sort_index()
        for month, count in month_counts.items():
            print(f"  {month}: {count} transactions")
            
        # Month distribution by user
        print("\nMonth distribution by user:")
        user_ids = df['User_id'].unique()
        for user_id in user_ids:
            user_df = df[df['User_id'] == user_id]
            user_months = user_df.groupby('month').size().sort_index()
            print(f"\nUser {user_id} has {len(user_months)} months of data:")
            for month, count in user_months.items():
                print(f"  {month}: {count} transactions")
    
    except Exception as e:
        print(f"Error analyzing transaction data: {e}")

if __name__ == "__main__":
    analyze_transaction_data()
