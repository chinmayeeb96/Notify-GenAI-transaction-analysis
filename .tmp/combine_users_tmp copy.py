# Combine all the user data files into a single file
import os
import pandas as pd

def combine_user_data(path):
    combined_data = []
    # Get list of CSV files
    csv_files = [f for f in sorted(os.listdir(path)) 
                if f.endswith('.csv')]

    if csv_files:
        for filename in csv_files:
            file_path = os.path.join(path, filename)
            # Read CSV file using pandas (this will automatically handle the header)
            df = pd.read_csv(file_path)
            combined_data.append(df)

    if combined_data:
        # Combine all dataframes
        final_df = pd.concat(combined_data, ignore_index=True)
        # Save to CSV - pandas will automatically write the column names as the first row
        output_path = os.path.join(path, f'combined_transaction_data.csv')
        final_df.to_csv(output_path, index=False)


if __name__ == "__main__":
    # Use absolute path to data directory
    base_path = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.join(base_path, "transaction_data")
    
    combine_user_data(data_path)
    print(f"Process completed. Check {data_path} for the output file.")
