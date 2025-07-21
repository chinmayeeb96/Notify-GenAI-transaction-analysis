#!/usr/bin/env python3
"""
This script measures the impact of filtering transaction data on context window size
"""
import sys
import json
import pandas as pd
import boto3
from io import StringIO

def read_csv_from_s3(bucket_name, key):
    """Read CSV file from S3 bucket"""
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        return pd.read_csv(StringIO(csv_content))
    except Exception as e:
        print(f"Error reading {key} from S3: {e}")
        return pd.read_csv(f"data/data_generation/{key.split('/')[-1]}")

def estimate_token_size(data):
    """Roughly estimate token size"""
    json_str = json.dumps(data, default=str)
    char_count = len(json_str)
    # Approximately 4 characters per token for English text
    estimated_tokens = char_count / 4
    return int(estimated_tokens)

def get_user_transactions(user_id, transactions_df):
    """Get transactions for a user"""
    return transactions_df[transactions_df['User_id'] == user_id].copy()

def main():
    print("Measuring context window optimization impact...")
    
    # Load transaction data (try S3 first, fall back to local)
    try:
        transactions_df = read_csv_from_s3("notifi-transaction-dataset", "notifi-dump/transaction_data_final.csv")
    except:
        print("Falling back to local transaction data...")
        transactions_df = pd.read_csv("data/data_generation/transaction_data/transaction_data_final.csv")
    
    # Get test user transactions
    user_id = "U1"
    user_transactions = get_user_transactions(user_id, transactions_df)
    
    # Full transaction fields
    full_transactions = user_transactions.to_dict('records')
    full_tokens = estimate_token_size(full_transactions)
    full_chars = len(json.dumps(full_transactions, default=str))
    
    # Optimized with only essential fields
    essential_fields = ['User_id', 'Amount', 'Txn Date', 'Category', 'Mode', 'Merchant Name']
    if not all(field in user_transactions.columns for field in essential_fields):
        # Adapt field names to what's actually in the data
        available_fields = user_transactions.columns.tolist()
        print(f"Available fields: {available_fields}")
        essential_fields = [f for f in available_fields if any(ef in f for ef in ['User', 'Amount', 'Date', 'Category', 'Mode', 'Merchant'])]
    
    optimized_transactions = user_transactions[essential_fields].to_dict('records')
    optimized_tokens = estimate_token_size(optimized_transactions)
    optimized_chars = len(json.dumps(optimized_transactions, default=str))
    
    # Calculate savings
    token_reduction = full_tokens - optimized_tokens
    token_reduction_pct = (token_reduction / full_tokens) * 100 if full_tokens > 0 else 0
    
    char_reduction = full_chars - optimized_chars
    char_reduction_pct = (char_reduction / full_chars) * 100 if full_chars > 0 else 0
    
    # Print results
    print("\n" + "="*50)
    print(f"Context Window Optimization Results for User {user_id}")
    print("="*50)
    print(f"Total transactions: {len(user_transactions)}")
    print(f"Full fields per transaction: {len(user_transactions.columns)}")
    print(f"Essential fields per transaction: {len(essential_fields)}")
    print("\nSize comparison:")
    print(f"Full transaction data: {full_chars:,} chars / ~{full_tokens:,} tokens")
    print(f"Optimized transaction data: {optimized_chars:,} chars / ~{optimized_tokens:,} tokens")
    print(f"Reduction: {char_reduction:,} chars / ~{token_reduction:,} tokens ({token_reduction_pct:.1f}% smaller)")
    
    # Print a sample transaction to see difference
    print("\nFull transaction sample:")
    print(json.dumps(full_transactions[0], indent=2, default=str))
    
    print("\nOptimized transaction sample:")
    print(json.dumps(optimized_transactions[0], indent=2, default=str))
    
    # Calculate Claude context limit
    claude_limit = 100000  # tokens
    full_pct_of_limit = (full_tokens / claude_limit) * 100
    optimized_pct_of_limit = (optimized_tokens / claude_limit) * 100
    
    print("\nContext window impact:")
    print(f"Full data uses approximately {full_pct_of_limit:.1f}% of Claude's context window")
    print(f"Optimized data uses approximately {optimized_pct_of_limit:.1f}% of Claude's context window")
    
    if full_pct_of_limit > 80:
        print("\nWARNING: Full transaction data might exceed Claude's context window!")
    elif full_pct_of_limit > 50:
        print("\nCAUTION: Full transaction data uses a significant portion of Claude's context window.")
    
    if optimized_pct_of_limit < 50:
        print("GOOD: Optimized data leaves plenty of room for other context and responses.")

if __name__ == "__main__":
    main()
