# Combine all the user data files into a single file
import os
import pandas as pd

def combine_user_data(path, uid_range, start_range):
    combined_data = []
    # Get list of CSV files excluding the output file
    csv_files = [f for f in sorted(os.listdir(path)) 
                if f.endswith('.csv') and not f.startswith(uid_range)]
    
    # Process each file
    if csv_files:
        for i, filename in enumerate(csv_files):
            file_path = os.path.join(path, filename)
            # Read CSV file using pandas (this will automatically handle the header)
            df = pd.read_csv(file_path)
            # Calculate the new user_id (U5 onwards)
            user_id = f"U{i+start_range}"  # Start from U5
            # Update the user_id column
            df['User Id'] = user_id
            combined_data.append(df)
    
    if combined_data:
        # Combine all dataframes
        final_df = pd.concat(combined_data, ignore_index=True)
        # Save to CSV - pandas will automatically write the column names as the first row
        output_path = os.path.join(path, f'{uid_range}_data.csv')
        final_df.to_csv(output_path, index=False)


if __name__ == "__main__":
    path, uid_range, start_range = "transaction_data/rahul", 'U5_to_U8', 5 # --> U5_to_U8_data.csv
    combine_user_data(path, uid_range, start_range)
    print(f"User data combined successfully into {uid_range}_data.csv")

    path, uid_range, start_range = "transaction_data/krishna", 'U9_to_U12', 9 # --> U9_to_U12_data.csv
    combine_user_data(path, uid_range, start_range)
    print(f"User data combined successfully into {uid_range}_data.csv")