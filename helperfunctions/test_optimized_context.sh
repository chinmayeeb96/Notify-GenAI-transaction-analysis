#!/bin/bash

# Test context window optimization with filtered fields
python3 -c "
import sys
import json
sys.path.append('.')
from main_pipeline import run_pipeline
from fetch_user_ids import get_user_ids
from agents.agent_template import estimate_token_size

# Modify get_user_ids to return only one user for testing
def get_single_user():
    return ['U1']

# Import and override the function
import fetch_user_ids
fetch_user_ids.get_user_ids = get_single_user

# Define a custom context size measurement function
def measure_transaction_context(df, title):
    # Measure full context size
    full_df = df.to_dict('records')
    full_tokens = estimate_token_size(full_df)
    
    # Create optimized version with only essential fields
    essential_fields = ['User_id', 'Amount', 'Txn Date', 'Category', 'Mode', 'Merchant Name']
    optimized_df = df[essential_fields].to_dict('records')
    optimized_tokens = estimate_token_size(optimized_df)
    
    # Calculate savings
    savings_pct = ((full_tokens - optimized_tokens) / full_tokens) * 100 if full_tokens > 0 else 0
    
    print(f\"\\n{'-'*50}\\n{title}\\n{'-'*50}\")
    print(f\"Full context size: {full_tokens:,} tokens ({len(full_df)} records)\")
    print(f\"Optimized context size: {optimized_tokens:,} tokens\")
    print(f\"Token reduction: {full_tokens - optimized_tokens:,} tokens ({savings_pct:.1f}% smaller)\")
    
    # Print sample of both for comparison
    print(f\"\\nFull record sample:\")
    print(json.dumps(full_df[0], indent=2, default=str)[:500] + \"...\")
    
    print(f\"\\nOptimized record sample:\")
    print(json.dumps(optimized_df[0], indent=2, default=str))
    
    return optimized_df

# Monkey patch the fetch_user_transactions function
from fetch_user_transactions import get_user_transactions as original_get_user_transactions

def optimized_get_user_transactions(user_id):
    # Get original transactions
    txns = original_get_user_transactions(user_id)
    
    # Measure and optimize
    print(f\"\\n📊 CONTEXT WINDOW OPTIMIZATION FOR USER {user_id}\\n\")
    measure_transaction_context(txns, \"Transaction Data Context Size\")
    
    # Return only essential fields to reduce context window
    essential_fields = ['User_id', 'Amount', 'Txn Date', 'Category', 'Mode', 'Merchant Name']
    return txns[essential_fields]

# Apply the monkey patch
import fetch_user_transactions
fetch_user_transactions.get_user_transactions = optimized_get_user_transactions

# Run pipeline with single user
run_pipeline()
"

echo "Testing complete. Check output for context window improvements."
