import pandas as pd
import re
from datetime import datetime
import calendar

def fix_transaction_dates(input_file, output_file):
    """
    Fix transaction dates for users U1, U2, U3, and U4 based on transaction ID patterns.
    
    Args:
        input_file (str): Path to the input CSV file
        output_file (str): Path to the output CSV file where fixed data will be saved
    """
    print(f"Loading transaction data from {input_file}")
    df = pd.read_csv(input_file)
    
    # Create a copy of the original data
    df_fixed = df.copy()
    
    # Convert date column to datetime
    df_fixed['Txn Date'] = pd.to_datetime(df_fixed['Txn Date'], errors='coerce')
    
    # Count transactions before fixing
    print("\nBefore fixing:")
    for user_id in ['U1', 'U2', 'U3', 'U4']:
        user_df = df_fixed[df_fixed['User_id'] == user_id]
        month_counts = user_df.groupby(user_df['Txn Date'].dt.strftime('%Y-%m')).size().sort_index()
        print(f"User {user_id} months: {', '.join(f'{m}:{c}' for m, c in month_counts.items())}")
    
    # Targeted users that need fixing
    target_users = ['U1', 'U2', 'U3', 'U4']
    fixed_count = 0
    
    # Process only the target users
    for user_id in target_users:
        # Get indices of rows for this user
        user_indices = df_fixed[df_fixed['User_id'] == user_id].index
        
        # Process each transaction for this user
        for idx in user_indices:
            txn_id = df_fixed.loc[idx, 'Txn ID']
            
            # Extract month from transaction ID (5th character)
            if len(txn_id) >= 5:
                try:
                    txn_month = int(txn_id[4])  # 0-indexed, so 4th position is the 5th character
                    
                    # Only process valid months (1-6)
                    if 1 <= txn_month <= 6:
                        # Get current date
                        current_date = df_fixed.loc[idx, 'Txn Date']
                        
                        # Check if the month from txn_id doesn't match the date's month
                        if current_date.month != txn_month:
                            # Handle day out of range issues
                            year = current_date.year
                            day = min(current_date.day, calendar.monthrange(year, txn_month)[1])
                            
                            # Create a new date with the correct month from txn_id
                            new_date = current_date.replace(month=txn_month, day=day)
                            
                            # Update the transaction date
                            df_fixed.loc[idx, 'Txn Date'] = new_date
                            print(f"Fixed: {txn_id} - Changed date from {current_date.strftime('%Y-%m-%d')} to {new_date.strftime('%Y-%m-%d')}")
                            fixed_count += 1
                except (ValueError, TypeError) as e:
                    print(f"Error processing {txn_id}: {e}")
    
    # Convert dates back to string format for CSV
    df_fixed['Txn Date'] = df_fixed['Txn Date'].dt.strftime('%Y-%m-%d')
    
    # Save the fixed data
    df_fixed.to_csv(output_file, index=False)
    print(f"\nFixed {fixed_count} transactions. Data saved to {output_file}")
    
    # Count transactions after fixing (convert back to datetime for analysis)
    print("\nAfter fixing:")
    df_fixed['Txn Date'] = pd.to_datetime(df_fixed['Txn Date'])
    for user_id in target_users:
        user_df = df_fixed[df_fixed['User_id'] == user_id]
        month_counts = user_df.groupby(user_df['Txn Date'].dt.strftime('%Y-%m')).size().sort_index()
        print(f"User {user_id} months: {', '.join(f'{m}:{c}' for m, c in month_counts.items())}")

if __name__ == "__main__":
    input_file = "/Users/chinmayee.dalai/Documents/genai_expo/data/data_generation/transaction_data/transaction_data_final.csv"
    output_file = "/Users/chinmayee.dalai/Documents/genai_expo/data/data_generation/transaction_data/transaction_data_fixed.csv"
    fix_transaction_dates(input_file, output_file)
