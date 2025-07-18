import boto3
import json
import csv
import pandas as pd
from datetime import datetime
import time
import os

class TransactionDataGenerator:
    def __init__(self, aws_access_key_id="keyid", aws_secret_access_key="access_key", aws_session_token ="token", region_name='us-east-1'):
       
       
        session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name
        )
        # Configure Bedrock client with timeout settings
        config = boto3.client('bedrock-runtime')._client_config
        config.read_timeout = 300  # 5 minutes timeout
        config.connect_timeout = 60  # 1 minute connect timeout
        self.bedrock = session.client('bedrock-runtime', config=config)
        print("✅ Using provided AWS credentials with extended timeout")
        
        
        # Model ID for Claude 3.5 Sonnet
        self.model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"
        
        # Model parameters
        self.max_tokens = 8000  # Reduced to avoid timeouts
        self.max_transactions_per_batch = 25  # Smaller batches to prevent timeouts
        
        # Define personas
        self.personas = [
            "Young Adult| Female| high spender| tech-savvy| upper middle class| Technology & Gaming| card holding 3| with multiple subscriptions",
            "Middle Age| Male| average spender| savings mindset| middle class| Fashion| card holding 2| with emi",
            "Senior| Female| conservative spender| retirement focused| fixed income| Healthcare & Groceries| card holding 1| minimal debt",
            "Young Professional| Male| moderate spender| investment focused| upper class| Travel & Dining| card holding 4| business expenses"
        ]
        
        # Define months
        self.months = ["january", "february", "march", "april", "may", "june"]
        
        # System prompt
        self.system_prompt = "You are a synthetic data generator, and you can generate data according to the instructions provided to you which resembles to real life data."
        
        # User prompt template for batch generation
        self.user_prompt_template = """Generate user transaction data for US based citizen for {month}, generate exactly {num_transactions} transactions. There should be some reoccurring transactions. Generate data in csv format. Include all the debit and credit transaction for an user, include salary credit as well. Make data realistic. 

Here is an example schema 
User Id,Txn ID,Txn Amount,Txn Date,Txn Category,Txn Mode,Merchant Code,Merchant Name,Payment Network,Issuer Name 
US001,B002,788,21/06/25,shopping,Credit Card,CK32,Amazon,VISA,BOA

Following is user characteristics, but you can add more characteristics:
persona: "{persona}"

User ID for all transactions should be: {user_id}

Here are list of categories, you can pick from the following:

INCOME_DIVIDENDS
INCOME_INTEREST_EARNED
INCOME_RETIREMENT_PENSION
INCOME_TAX_REFUND
INCOME_UNEMPLOYMENT
INCOME_WAGES
INCOME_OTHER_INCOME
TRANSFER_IN_CASH_ADVANCES_AND_LOANS
TRANSFER_IN_DEPOSIT
TRANSFER_IN_INVESTMENT_AND_RETIREMENT_FUNDS
TRANSFER_IN_SAVINGS
TRANSFER_IN_ACCOUNT_TRANSFER
TRANSFER_IN_OTHER_TRANSFER_IN
TRANSFER_OUT_INVESTMENT_AND_RETIREMENT_FUNDS
TRANSFER_OUT_SAVINGS
TRANSFER_OUT_WITHDRAWAL
TRANSFER_OUT_ACCOUNT_TRANSFER
TRANSFER_OUT_OTHER_TRANSFER_OUT
LOAN_PAYMENTS_CAR_PAYMENT
LOAN_PAYMENTS_CREDIT_CARD_PAYMENT
LOAN_PAYMENTS_PERSONAL_LOAN_PAYMENT
LOAN_PAYMENTS_MORTGAGE_PAYMENT
LOAN_PAYMENTS_STUDENT_LOAN_PAYMENT
LOAN_PAYMENTS_OTHER_PAYMENT
BANK_FEES_ATM_FEES
BANK_FEES_FOREIGN_TRANSACTION_FEES
BANK_FEES_INSUFFICIENT_FUNDS
BANK_FEES_INTEREST_CHARGE
BANK_FEES_OVERDRAFT_FEES
BANK_FEES_OTHER_BANK_FEES
ENTERTAINMENT_CASINOS_AND_GAMBLING
ENTERTAINMENT_MUSIC_AND_AUDIO
ENTERTAINMENT_SPORTING_EVENTS_AMUSEMENT_PARKS_AND_MUSEUMS
ENTERTAINMENT_TV_AND_MOVIES
ENTERTAINMENT_VIDEO_GAMES
ENTERTAINMENT_OTHER_ENTERTAINMENT
FOOD_AND_DRINK_BEER_WINE_AND_LIQUOR
FOOD_AND_DRINK_COFFEE
FOOD_AND_DRINK_FAST_FOOD
FOOD_AND_DRINK_GROCERIES
FOOD_AND_DRINK_RESTAURANT
FOOD_AND_DRINK_VENDING_MACHINES
FOOD_AND_DRINK_OTHER_FOOD_AND_DRINK
GENERAL_MERCHANDISE_BOOKSTORES_AND_NEWSSTANDS
GENERAL_MERCHANDISE_CLOTHING_AND_ACCESSORIES
GENERAL_MERCHANDISE_CONVENIENCE_STORES
GENERAL_MERCHANDISE_DEPARTMENT_STORES
GENERAL_MERCHANDISE_DISCOUNT_STORES
GENERAL_MERCHANDISE_ELECTRONICS
GENERAL_MERCHANDISE_GIFTS_AND_NOVELTIES
GENERAL_MERCHANDISE_OFFICE_SUPPLIES
GENERAL_MERCHANDISE_ONLINE_MARKETPLACES
GENERAL_MERCHANDISE_PET_SUPPLIES
GENERAL_MERCHANDISE_SPORTING_GOODS
GENERAL_MERCHANDISE_SUPERSTORES
GENERAL_MERCHANDISE_TOBACCO_AND_VAPE
GENERAL_MERCHANDISE_OTHER_GENERAL_MERCHANDISE
HOME_IMPROVEMENT_FURNITURE
HOME_IMPROVEMENT_HARDWARE
HOME_IMPROVEMENT_REPAIR_AND_MAINTENANCE
HOME_IMPROVEMENT_SECURITY
HOME_IMPROVEMENT_OTHER_HOME_IMPROVEMENT
MEDICAL_DENTAL_CARE
MEDICAL_EYE_CARE
MEDICAL_NURSING_CARE
MEDICAL_PHARMACIES_AND_SUPPLEMENTS
MEDICAL_PRIMARY_CARE
MEDICAL_VETERINARY_SERVICES
MEDICAL_OTHER_MEDICAL
PERSONAL_CARE_GYMS_AND_FITNESS_CENTERS
PERSONAL_CARE_HAIR_AND_BEAUTY
PERSONAL_CARE_LAUNDRY_AND_DRY_CLEANING
PERSONAL_CARE_OTHER_PERSONAL_CARE
GENERAL_SERVICES_ACCOUNTING_AND_FINANCIAL_PLANNING
GENERAL_SERVICES_AUTOMOTIVE
GENERAL_SERVICES_CHILDCARE
GENERAL_SERVICES_CONSULTING_AND_LEGAL
GENERAL_SERVICES_EDUCATION
GENERAL_SERVICES_INSURANCE
GENERAL_SERVICES_POSTAGE_AND_SHIPPING
GENERAL_SERVICES_STORAGE
GENERAL_SERVICES_OTHER_GENERAL_SERVICES
GOVERNMENT_AND_NON_PROFIT_DONATIONS
GOVERNMENT_AND_NON_PROFIT_GOVERNMENT_DEPARTMENTS_AND_AGENCIES
GOVERNMENT_AND_NON_PROFIT_TAX_PAYMENT
GOVERNMENT_AND_NON_PROFIT_OTHER_GOVERNMENT_AND_NON_PROFIT
TRANSPORTATION_BIKES_AND_SCOOTERS
TRANSPORTATION_GAS
TRANSPORTATION_PARKING
TRANSPORTATION_PUBLIC_TRANSIT
TRANSPORTATION_TAXIS_AND_RIDE_SHARES
TRANSPORTATION_TOLLS
TRANSPORTATION_OTHER_TRANSPORTATION
TRAVEL_FLIGHTS
TRAVEL_LODGING
TRAVEL_RENTAL_CARS
TRAVEL_OTHER_TRAVEL
RENT_AND_UTILITIES_GAS_AND_ELECTRICITY
RENT_AND_UTILITIES_INTERNET_AND_CABLE
RENT_AND_UTILITIES_RENT
RENT_AND_UTILITIES_SEWAGE_AND_WASTE_MANAGEMENT
RENT_AND_UTILITIES_TELEPHONE
RENT_AND_UTILITIES_WATER
RENT_AND_UTILITIES_OTHER_UTILITIES

Please ensure the output is ONLY the CSV data with proper headers and no additional text or explanations."""

    def call_bedrock_model(self, user_prompt, retry_count=0, max_retries=3):
        """Call AWS Bedrock model to generate transaction data with retry logic"""
        try:
            print(f"    Making API call (attempt {retry_count + 1}/{max_retries + 1})...")
            
            # Prepare the request payload
            request_body = {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": self.max_tokens,
                "system": self.system_prompt,
                "messages": [
                    {
                        "role": "user",
                        "content": user_prompt
                    }
                ],
                "temperature": 0.7
            }
            
            # Call the model
            response = self.bedrock.invoke_model(
                modelId=self.model_id,
                body=json.dumps(request_body),
                contentType="application/json"
            )
            
            # Parse the response
            response_body = json.loads(response['body'].read())
            print(f"    ✅ API call successful")
            return response_body['content'][0]['text']
            
        except Exception as e:
            error_msg = str(e)
            print(f"    ❌ API call failed: {error_msg}")
            
            # Check if it's a timeout or rate limit error and retry
            if ("timeout" in error_msg.lower() or "throttling" in error_msg.lower()) and retry_count < max_retries:
                wait_time = (retry_count + 1) * 10  # Exponential backoff: 10s, 20s, 30s
                print(f"    ⏳ Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
                return self.call_bedrock_model(user_prompt, retry_count + 1, max_retries)
            
            return None

    def parse_csv_from_response(self, response_text):
        """Parse CSV data from the model response"""
        try:
            # Split the response into lines
            lines = response_text.strip().split('\n')
            
            # Find the CSV data (look for header line)
            csv_start = 0
            for i, line in enumerate(lines):
                if 'User Id' in line or 'UserId' in line or line.count(',') > 5:
                    csv_start = i
                    break
            
            # Extract CSV lines
            csv_lines = []
            for line in lines[csv_start:]:
                line = line.strip()
                if line and ',' in line:
                    csv_lines.append(line)
            
            return csv_lines
            
        except Exception as e:
            print(f"Error parsing CSV from response: {str(e)}")
            return []

    def generate_data_for_persona_month(self, persona, month, user_id):
        """Generate transaction data for a specific persona and month using batch processing"""
        print(f"Generating data for User {user_id}, Month: {month}")
        
        total_transactions = 80  # Target total transactions per month
        batches = []
        remaining_transactions = total_transactions
        
        # Split into batches to avoid token limits
        while remaining_transactions > 0:
            batch_size = min(self.max_transactions_per_batch, remaining_transactions)
            batches.append(batch_size)
            remaining_transactions -= batch_size
        
        print(f"  Splitting into {len(batches)} batches: {batches}")
        
        all_csv_lines = []
        headers_set = False
        
        for batch_idx, num_transactions in enumerate(batches, 1):
            print(f"  Processing batch {batch_idx}/{len(batches)} ({num_transactions} transactions)")
            
            # Format the user prompt for this batch
            user_prompt = self.user_prompt_template.format(
                month=month,
                persona=persona,
                user_id=user_id,
                num_transactions=num_transactions
            )
            
            # Call the model
            response = self.call_bedrock_model(user_prompt)
            
            if response:
                # Parse CSV data from response
                csv_lines = self.parse_csv_from_response(response)
                
                if csv_lines:
                    # For first batch, include headers
                    if not headers_set:
                        all_csv_lines.extend(csv_lines)
                        headers_set = True
                    else:
                        # Skip header line for subsequent batches
                        all_csv_lines.extend(csv_lines[1:] if len(csv_lines) > 1 else [])
                    
                    print(f"    Generated {len(csv_lines)-1 if headers_set and batch_idx == 1 else len(csv_lines)} transactions")
                else:
                    print(f"    No valid CSV data generated for batch {batch_idx}")
            else:
                print(f"    Failed to generate data for batch {batch_idx}")
            
            # Add delay between batches to avoid rate limiting
            if batch_idx < len(batches):
                print(f"    ⏳ Waiting 8 seconds before next batch...")
                time.sleep(8)  # Increased delay between batches
        
        if all_csv_lines:
            # Save individual CSV file
            filename = f"transaction_data_{user_id}_{month}.csv"
            with open(filename, 'w', newline='', encoding='utf-8') as file:
                file.write('\n'.join(all_csv_lines))
            
            print(f"  ✅ Generated {len(all_csv_lines)-1} total transactions for {filename}")
            return filename
        else:
            print(f"  ❌ No valid CSV data generated for User {user_id}, Month: {month}")
            return None

    def combine_all_csv_files(self, csv_files):
        """Combine all generated CSV files into one master file"""
        print("\nCombining all CSV files...")
        
        all_data = []
        headers = None
        
        for csv_file in csv_files:
            if csv_file and os.path.exists(csv_file):
                try:
                    df = pd.read_csv(csv_file)
                    
                    # Set headers from first file
                    if headers is None:
                        headers = df.columns.tolist()
                    
                    # Ensure consistent column names
                    df.columns = headers
                    
                    all_data.append(df)
                    print(f"Added {len(df)} transactions from {csv_file}")
                    
                except Exception as e:
                    print(f"Error reading {csv_file}: {str(e)}")
        
        if all_data:
            # Combine all dataframes
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # Save combined file
            combined_filename = f"combined_transaction_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            combined_df.to_csv(combined_filename, index=False)
            
            print(f"\nCombined file saved as: {combined_filename}")
            print(f"Total transactions: {len(combined_df)}")
            print(f"Total users: {combined_df['User Id'].nunique() if 'User Id' in combined_df.columns else 'Unknown'}")
            
            return combined_filename
        else:
            print("No data to combine")
            return None

    def generate_all_data(self):
        """Generate transaction data for all personas and months"""
        print("Starting transaction data generation...")
        print(f"Personas: {len(self.personas)}")
        print(f"Months: {len(self.months)}")
        print(f"Total combinations: {len(self.personas) * len(self.months)}")
        
        csv_files = []
        
        # Generate data for each persona and month combination
        for persona_idx, persona in enumerate(self.personas, 1):
            user_id = f"US{persona_idx:03d}"
            print(f"\n--- Processing Persona {persona_idx}: {persona[:50]}... ---")
            
            for month in self.months:
                # Generate data
                csv_file = self.generate_data_for_persona_month(persona, month, user_id)
                csv_files.append(csv_file)
                
                # Add delay to avoid rate limiting between users/months
                print(f"⏳ Waiting 10 seconds before next month...")
                time.sleep(10)  # Increased delay for better rate limiting
        
        # Combine all CSV files
        combined_file = self.combine_all_csv_files(csv_files)
        
        print(f"\nData generation completed!")
        if combined_file:
            print(f"Combined file: {combined_file}")
        
        return combined_file

def main():
    """Main function to run the data generation"""
    print("🚀 AWS Bedrock Transaction Data Generator")
    print("=" * 50)
    
    # Optional: Configure AWS credentials directly in code
    # Uncomment and fill these if you want to hardcode credentials
    # AWS_ACCESS_KEY_ID = "your_access_key_here"
    # AWS_SECRET_ACCESS_KEY = "your_secret_key_here"
    
    # Create generator instance
    # If using hardcoded credentials:
    # generator = TransactionDataGenerator(
    #     aws_access_key_id=AWS_ACCESS_KEY_ID,
    #     aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    # )
    
    # Using default credential chain (recommended)
    generator = TransactionDataGenerator()
    
    print(f"📋 Configuration:")
    print(f"   Max tokens per request: {generator.max_tokens}")
    print(f"   Max transactions per batch: {generator.max_transactions_per_batch}")
    print(f"   Total personas: {len(generator.personas)}")
    print(f"   Total months: {len(generator.months)}")
    print(f"   Expected files: {len(generator.personas) * len(generator.months)}")
    
    # Generate all data
    combined_file = generator.generate_all_data()
    
    if combined_file:
        print(f"\n✅ Success! Combined transaction data saved to: {combined_file}")
        
        # Display basic statistics
        try:
            df = pd.read_csv(combined_file)
            print(f"\n📊 Data Summary:")
            print(f"   Total Transactions: {len(df)}")
            print(f"   Unique Users: {df.iloc[:, 0].nunique()}")  # First column should be User ID
            print(f"   Date Range: {df.iloc[:, 3].min()} to {df.iloc[:, 3].max()}")  # Assuming date is 4th column
            print(f"   File Size: {os.path.getsize(combined_file) / 1024:.2f} KB")
            
            # Show transaction count per user
            user_counts = df.iloc[:, 0].value_counts()
            print(f"\n📈 Transactions per User:")
            for user, count in user_counts.items():
                print(f"   {user}: {count} transactions")
                
        except Exception as e:
            print(f"Error reading combined file for statistics: {str(e)}")
    else:
        print("\n❌ Data generation failed!")

if __name__ == "__main__":
    main()