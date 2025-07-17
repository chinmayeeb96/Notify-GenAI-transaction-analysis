import pandas as pd
import os
import glob
from datetime import datetime

class TransactionMerger:
    def __init__(self):
        """Initialize the Transaction Merger"""
        self.transaction_files = []
        self.combined_data = []
        
    def find_transaction_files(self, pattern="transaction_data_*.csv"):
        """Find all transaction CSV files in the current directory"""
        self.transaction_files = glob.glob(pattern)
        self.transaction_files.sort()  # Sort for consistent processing
        print(f"Found {len(self.transaction_files)} transaction files:")
        for file in self.transaction_files:
            print(f"  - {file}")
        return self.transaction_files
    
    def generate_unique_txn_id(self, user_id, original_txn_id, counter):
        """Generate a unique transaction ID"""
        # Extract user number from user_id (e.g., US001 -> 001)
        user_num = user_id.replace('US', '')
        
        # Create unique ID: UserNum + Counter (padded to 6 digits)
        unique_id = f"{user_num}{counter:06d}"
        return unique_id
    
    def process_file(self, file_path):
        """Process a single transaction file and make transaction IDs unique"""
        print(f"\nProcessing: {file_path}")
        
        try:
            # Read the CSV file
            df = pd.read_csv(file_path)
            
            if df.empty:
                print(f"  ⚠️  File is empty: {file_path}")
                return None
            
            print(f"  📊 Found {len(df)} transactions")
            
            # Get the user ID from the first row
            user_id = df.iloc[0]['User Id'] if 'User Id' in df.columns else df.iloc[0]['User ID']
            
            # Check for duplicate transaction IDs within this file
            original_txn_count = len(df)
            duplicate_txns = df['Txn ID'].duplicated().sum()
            
            if duplicate_txns > 0:
                print(f"  ⚠️  Found {duplicate_txns} duplicate transaction IDs in file")
            
            # Generate unique transaction IDs
            global_counter = getattr(self, 'global_counter', 0)
            
            new_txn_ids = []
            for i, original_txn_id in enumerate(df['Txn ID']):
                global_counter += 1
                new_id = self.generate_unique_txn_id(user_id, original_txn_id, global_counter)
                new_txn_ids.append(new_id)
            
            # Update the global counter
            self.global_counter = global_counter
            
            # Replace transaction IDs with unique ones
            df['Txn ID'] = new_txn_ids
            
            # Standardize column names
            df.columns = df.columns.str.strip()  # Remove whitespace
            if 'User ID' in df.columns:
                df.rename(columns={'User ID': 'User Id'}, inplace=True)
            
            # Sort by date if possible
            try:
                # Try to parse dates in different formats
                df['Txn Date'] = pd.to_datetime(df['Txn Date'], errors='coerce', infer_datetime_format=True)
                df = df.sort_values('Txn Date')
                print(f"  📅 Sorted transactions by date")
            except:
                print(f"  ⚠️  Could not parse dates for sorting")
            
            print(f"  ✅ Generated unique transaction IDs: {new_txn_ids[0]} to {new_txn_ids[-1]}")
            
            return df
            
        except Exception as e:
            print(f"  ❌ Error processing {file_path}: {str(e)}")
            return None
    
    def merge_all_files(self):
        """Merge all transaction files into one combined dataset"""
        print("\n" + "="*60)
        print("MERGING ALL TRANSACTION FILES")
        print("="*60)
        
        # Initialize global counter
        self.global_counter = 0
        
        # Process each file
        for file_path in self.transaction_files:
            df = self.process_file(file_path)
            if df is not None:
                self.combined_data.append(df)
        
        if not self.combined_data:
            print("\n❌ No data to merge!")
            return None
        
        # Combine all dataframes
        print(f"\n🔄 Combining {len(self.combined_data)} datasets...")
        combined_df = pd.concat(self.combined_data, ignore_index=True)
        
        # Final validation
        print(f"\n📊 MERGE SUMMARY:")
        print(f"   Total transactions: {len(combined_df)}")
        print(f"   Unique users: {combined_df['User Id'].nunique()}")
        print(f"   Unique transaction IDs: {combined_df['Txn ID'].nunique()}")
        print(f"   Date range: {combined_df['Txn Date'].min()} to {combined_df['Txn Date'].max()}")
        
        # Check for any remaining duplicates
        remaining_duplicates = combined_df['Txn ID'].duplicated().sum()
        if remaining_duplicates > 0:
            print(f"   ⚠️  WARNING: {remaining_duplicates} duplicate transaction IDs found!")
        else:
            print(f"   ✅ All transaction IDs are unique!")
        
        # Show transactions per user
        user_counts = combined_df['User Id'].value_counts().sort_index()
        print(f"\n📈 TRANSACTIONS PER USER:")
        for user, count in user_counts.items():
            print(f"   {user}: {count} transactions")
        
        return combined_df
    
    def save_merged_data(self, combined_df, output_filename=None):
        """Save the merged data to a CSV file"""
        if output_filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f"merged_transactions_{timestamp}.csv"
        
        print(f"\n💾 Saving merged data to: {output_filename}")
        
        try:
            combined_df.to_csv(output_filename, index=False)
            file_size = os.path.getsize(output_filename) / 1024  # Size in KB
            print(f"   ✅ File saved successfully!")
            print(f"   📁 File size: {file_size:.2f} KB")
            print(f"   📍 Location: {os.path.abspath(output_filename)}")
            return output_filename
        except Exception as e:
            print(f"   ❌ Error saving file: {str(e)}")
            return None
    
    def generate_summary_report(self, combined_df, output_filename):
        """Generate a summary report of the merged data"""
        report_filename = output_filename.replace('.csv', '_summary.txt')
        
        print(f"\n📋 Generating summary report: {report_filename}")
        
        try:
            with open(report_filename, 'w') as f:
                f.write("TRANSACTION DATA MERGE SUMMARY REPORT\n")
                f.write("="*50 + "\n\n")
                f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
                f.write(f"Source files processed: {len(self.transaction_files)}\n\n")
                
                f.write("OVERALL STATISTICS:\n")
                f.write("-"*20 + "\n")
                f.write(f"Total transactions: {len(combined_df):,}\n")
                f.write(f"Unique users: {combined_df['User Id'].nunique()}\n")
                f.write(f"Unique transaction IDs: {combined_df['Txn ID'].nunique()}\n")
                f.write(f"Date range: {combined_df['Txn Date'].min()} to {combined_df['Txn Date'].max()}\n\n")
                
                f.write("TRANSACTIONS PER USER:\n")
                f.write("-"*25 + "\n")
                user_counts = combined_df['User Id'].value_counts().sort_index()
                for user, count in user_counts.items():
                    f.write(f"{user}: {count:,} transactions\n")
                
                f.write("\nTRANSACTION CATEGORIES:\n")
                f.write("-"*25 + "\n")
                category_counts = combined_df['Txn Category'].value_counts().head(10)
                for category, count in category_counts.items():
                    f.write(f"{category}: {count:,} transactions\n")
                
                f.write("\nSOURCE FILES:\n")
                f.write("-"*15 + "\n")
                for file in self.transaction_files:
                    f.write(f"- {file}\n")
            
            print(f"   ✅ Summary report saved!")
            
        except Exception as e:
            print(f"   ❌ Error generating summary report: {str(e)}")

def main():
    """Main function to merge transaction files"""
    print("🔄 TRANSACTION FILE MERGER")
    print("="*40)
    
    # Create merger instance
    merger = TransactionMerger()
    
    # Find all transaction files
    files = merger.find_transaction_files()
    
    if not files:
        print("\n❌ No transaction files found!")
        print("   Make sure you have files matching pattern: transaction_data_*.csv")
        return
    
    # Merge all files
    combined_df = merger.merge_all_files()
    
    if combined_df is None:
        print("\n❌ Merge failed!")
        return
    
    # Save merged data
    output_file = merger.save_merged_data(combined_df)
    
    if output_file:
        # Generate summary report
        merger.generate_summary_report(combined_df, output_file)
        
        print(f"\n🎉 SUCCESS!")
        print(f"   📁 Merged file: {output_file}")
        print(f"   📋 Summary report: {output_file.replace('.csv', '_summary.txt')}")
        print(f"   📊 Total transactions: {len(combined_df):,}")
        print(f"   👥 Total users: {combined_df['User Id'].nunique()}")
    else:
        print(f"\n❌ Failed to save merged data!")

if __name__ == "__main__":
    main()
