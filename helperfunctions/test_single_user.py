from main_pipeline import process_user
from fetch_user_transactions import get_user_transactions
import boto3
import pandas as pd
from io import StringIO

def test_single_user(user_id):
    """
    Test the pipeline on a single user
    """
    try:
        # S3 configuration
        S3_BUCKET = "notifi-transaction-dataset"
        S3_PREFIX = "notifi-dump/"
        
        # Read user data
        print(f"Loading user data for {user_id}...")
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}user.csv")
        csv_content = response['Body'].read().decode('utf-8')
        userinfo_df = pd.read_csv(StringIO(csv_content))
        
        # Get user info
        user_info = userinfo_df[userinfo_df['User_id'] == user_id].iloc[0].to_dict()
        
        # Load product data
        print("Loading product data...")
        product_data = {
            'coupons': pd.read_csv(StringIO(s3_client.get_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}product_coupons_data.csv")['Body'].read().decode('utf-8'))).to_dict(orient='records'),
            'loans': pd.read_csv(StringIO(s3_client.get_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}loan_data.csv")['Body'].read().decode('utf-8'))).to_dict(orient='records'),
            'credit_cards': pd.read_csv(StringIO(s3_client.get_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}credit_card_data.csv")['Body'].read().decode('utf-8'))).to_dict(orient='records'),
            'savings': pd.read_csv(StringIO(s3_client.get_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}high_yield_savings_data.csv")['Body'].read().decode('utf-8'))).to_dict(orient='records')
        }
        
        # Get user transactions
        transactions = get_user_transactions(user_id)
        
        # Process user
        print(f"\nProcessing user {user_id}...")
        output = process_user(user_id, user_info, transactions, product_data)
        
        print(f"\nProcessing complete for user {user_id}")
        
    except Exception as e:
        print(f"Error processing user {user_id}: {e}")

# Test with user that has all 6 months
if __name__ == "__main__":
    test_single_user("U5")  # User with 6 months of data
