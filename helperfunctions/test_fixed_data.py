from main_pipeline import process_user
from fetch_user_transactions import get_user_transactions
import boto3
import pandas as pd
from io import StringIO
import os

def test_pipeline_with_fixed_data():
    """
    Test the pipeline with the fixed transaction data
    """
    # Define the fixed data path
    fixed_data_path = "/Users/chinmayee.dalai/Documents/genai_expo/data/data_generation/transaction_data/transaction_data_fixed.csv"
    
    # Override the S3 fetching to use local file instead
    def get_local_user_transactions(user_id):
        """Override to use the fixed local data"""
        try:
            # Read from the fixed local file
            df = pd.read_csv(fixed_data_path)
            user_df = df[df['User_id'] == user_id]
            
            # Convert date and check month distribution
            user_df['Txn Date'] = pd.to_datetime(user_df['Txn Date'], errors='coerce')
            
            # Count transactions by month
            month_counts = user_df.groupby(user_df['Txn Date'].dt.strftime('%Y-%m')).size().sort_index()
            print(f"Found {len(month_counts)} months of data for user {user_id}:")
            for month, count in sorted(month_counts.items()):
                print(f"  {month}: {count} transactions")
                
            return user_df
        except Exception as e:
            print(f"Error reading transactions for user {user_id} from local file: {e}")
            return pd.DataFrame()
    
    # Store original function
    original_get_user_transactions = get_user_transactions
    
    try:
        # Use our local override
        import fetch_user_transactions
        fetch_user_transactions.get_user_transactions = get_local_user_transactions
        
        # S3 configuration for other data
        S3_BUCKET = "notifi-transaction-dataset"
        S3_PREFIX = "notifi-dump/"
        
        # Read user data
        print(f"Loading user data...")
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}user.csv")
        csv_content = response['Body'].read().decode('utf-8')
        userinfo_df = pd.read_csv(StringIO(csv_content))
        
        # Load product data
        print("Loading product data...")
        product_data = {
            'coupons': pd.read_csv(StringIO(s3_client.get_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}product_coupons_data.csv")['Body'].read().decode('utf-8'))).to_dict(orient='records'),
            'loans': pd.read_csv(StringIO(s3_client.get_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}loan_data.csv")['Body'].read().decode('utf-8'))).to_dict(orient='records'),
            'credit_cards': pd.read_csv(StringIO(s3_client.get_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}credit_card_data.csv")['Body'].read().decode('utf-8'))).to_dict(orient='records'),
            'savings': pd.read_csv(StringIO(s3_client.get_object(Bucket=S3_BUCKET, Key=f"{S3_PREFIX}high_yield_savings_data.csv")['Body'].read().decode('utf-8'))).to_dict(orient='records')
        }
        
        # Create a special output directory for this test
        output_dir = "output_fixed_data"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        # Save original output path
        original_output_dir = "output"
        
        # Redirect output to our test directory
        def process_test_user(user_id, user_info, transactions, product_data):
            # Store original output path
            import main_pipeline
            original_open = open
            
            def patched_open(file, *args, **kwargs):
                if file.startswith(f"{original_output_dir}/output_"):
                    # Redirect to our test directory
                    new_file = file.replace(f"{original_output_dir}/", f"{output_dir}/")
                    return original_open(new_file, *args, **kwargs)
                return original_open(file, *args, **kwargs)
            
            # Patch the built-in open function
            import builtins
            builtins.open = patched_open
            
            try:
                # Process the user with the original function
                return process_user(user_id, user_info, transactions, product_data)
            finally:
                # Restore original open
                builtins.open = original_open
        
        # Test the pipeline for the users that were fixed
        for user_id in ['U1', 'U2', 'U3', 'U4']:
            print(f"\n{'='*50}")
            print(f"Testing User {user_id} with fixed data")
            print(f"{'='*50}")
            
            # Get user info
            user_info = userinfo_df[userinfo_df['User_id'] == user_id].iloc[0].to_dict()
            
            # Get user transactions from the fixed data
            transactions = get_local_user_transactions(user_id)
            
            # Process user
            process_test_user(user_id, user_info, transactions, product_data)
            
        print(f"\nTest complete! Check the {output_dir} directory for the results.")
            
    finally:
        # Restore original function
        fetch_user_transactions.get_user_transactions = original_get_user_transactions

if __name__ == "__main__":
    test_pipeline_with_fixed_data()
