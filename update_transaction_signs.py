import pandas as pd

# Read the CSV file
df = pd.read_csv('data/data_generation/transaction_data/transaction_data_finalpart1.csv')

# Update transaction signs: Direct Deposit will be negative, all others positive
df['Txn Amount'] = df.apply(lambda row: -abs(row['Txn Amount']) 
                           if row['Txn Category'] == 'INCOME_WAGES'
                           else abs(row['Txn Amount']), axis=1)

# Save the updated CSV
output_file = 'data/data_generation/transaction_data/transaction_data_final_with_signs.csv'
df.to_csv(output_file, index=False)
print(f"Updated transaction signs and saved to: {output_file}")

# Display some examples
print("\nSample transactions:")
print(df[['Txn Category', 'Txn Amount']].head(10))
