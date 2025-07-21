from fetch_user_ids import get_user_ids
from fetch_user_transactions import get_user_transactions
from agents import coupons_agent, agent_template, credit_cards_agent, financial_summary_agent, loans_agent, savings_agent, email_notification_agent
from agents.agent_template import check_context_window_limit

from combine_outputs import build_final_output
import pandas as pd
import json
import boto3
from io import StringIO


def read_csv_from_s3(bucket_name, key):
    """
    Read CSV file from S3 bucket
    
    Args:
        bucket_name (str): S3 bucket name
        key (str): S3 object key (file path)
    
    Returns:
        pandas.DataFrame: DataFrame containing the CSV data
    """
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket_name, Key=key)
        csv_content = response['Body'].read().decode('utf-8')
        return pd.read_csv(StringIO(csv_content))
    except Exception as e:
        print(f"Error reading {key} from S3: {e}")
        raise


def preprocess_transactions(transactions):
    """
    Preprocess transaction data to ensure proper date formatting and field naming
    
    Args:
        transactions (pd.DataFrame): Raw transaction data
        
    Returns:
        pd.DataFrame: Preprocessed transaction data with month_year column
    """
    # Make a copy to avoid modifying the original
    transactions = transactions.copy()
    
    # Ensure date column is properly formatted
    transactions['Txn Date'] = pd.to_datetime(transactions['Txn Date'], errors='coerce')
    
    # Check for and report any invalid dates
    invalid_dates = transactions['Txn Date'].isna().sum()
    if invalid_dates > 0:
        print(f"Warning: {invalid_dates} transactions with invalid dates were removed")
        transactions = transactions.dropna(subset=['Txn Date'])
    
    # Extract month-year and ensure it's in correct format
    transactions['month_year'] = transactions['Txn Date'].dt.to_period('M')
    
    # Filter to include only relevant fields to reduce context window size
    relevant_fields = ['User_id', 'Txn Amount', 'Txn Date', 'Txn Category', 'Txn Mode', 'Merchant Name']
    if set(relevant_fields).issubset(set(transactions.columns)):
        transactions_filtered = transactions[relevant_fields].copy()
    else:
        # If column names don't match, try to map them
        column_mapping = {
            'Amount': 'Txn Amount',
            'Category': 'Txn Category',
            'Mode': 'Txn Mode'
        }
        transactions = transactions.rename(columns=column_mapping)
        transactions_filtered = transactions[relevant_fields].copy()
    
    # Add month_year to filtered transactions
    transactions_filtered['month_year'] = transactions['month_year']
    
    return transactions_filtered


def get_product_recommendations(user_info, transactions_for_agents, product_data):
    """
    Get product recommendations from different agents
    
    Args:
        user_info (dict): User information
        transactions_for_agents (pd.DataFrame): Preprocessed transaction data
        product_data (dict): Dictionary containing all product data
        
    Returns:
        dict: Dictionary containing all recommendations
    """
    # Get recommendations from each agent
    coupons_rec = coupons_agent.run_coupons_agent(user_info, transactions_for_agents, product_data['coupons'])
    loans_rec = loans_agent.run_loans_agent(user_info, transactions_for_agents, product_data['loans'])
    credit_rec = credit_cards_agent.run_credit_cards_agent(user_info, transactions_for_agents, product_data['credit_cards'])
    savings_rec = savings_agent.run_savings_agent(user_info, transactions_for_agents, product_data['savings'])
    
    # Process recommendations into standard format
    recommendations = {}
    
    # Process coupons recommendations
    try:
        recommendations['coupons'] = json.loads(coupons_rec) if isinstance(coupons_rec, str) else coupons_rec
        if not isinstance(recommendations['coupons'], list):
            recommendations['coupons'] = ["CO1", "CO2", "CO3"]
    except:
        recommendations['coupons'] = ["CO1", "CO2", "CO3"]
        
    # Process loans recommendations
    try:
        recommendations['loans'] = json.loads(loans_rec) if isinstance(loans_rec, str) else loans_rec
        if not isinstance(recommendations['loans'], list):
            recommendations['loans'] = ["LN1", "LN2", "LN3"]
    except:
        recommendations['loans'] = ["LN1", "LN2", "LN3"]
        
    # Process credit card recommendations
    try:
        recommendations['credit_cards'] = json.loads(credit_rec) if isinstance(credit_rec, str) else credit_rec
        if not isinstance(recommendations['credit_cards'], list):
            recommendations['credit_cards'] = ["CC1", "CC2", "CC3"]
    except:
        recommendations['credit_cards'] = ["CC1", "CC2", "CC3"]
        
    # Process savings recommendations
    try:
        recommendations['high_yield_savings'] = json.loads(savings_rec) if isinstance(savings_rec, str) else savings_rec
        if not isinstance(recommendations['high_yield_savings'], list):
            recommendations['high_yield_savings'] = ["HY1", "HY2", "HY3"]
    except:
        recommendations['high_yield_savings'] = ["HY1", "HY2", "HY3"]
    
    return recommendations


def generate_monthly_summaries(user_info, transactions):
    """
    Generate monthly summaries for all available months
    
    Args:
        user_info (dict): User information
        transactions (pd.DataFrame): Preprocessed transaction data
        
    Returns:
        list: List of monthly summary dictionaries
    """
    monthly_summary = []
    
    # Check all available months
    available_months = transactions['month_year'].unique()
    print(f"Found {len(available_months)} months of data: {', '.join(str(m) for m in sorted(available_months))}")
    
    # Group transactions by month and create monthly summaries
    for month_year in sorted(available_months):
        monthly_data = transactions[transactions['month_year'] == month_year].copy()
        
        # Using all available transactions for each month
        print(f"Processing all {len(monthly_data)} transactions for month {month_year}")
        
        # Convert datetime columns to strings for the agent
        monthly_data_for_agent = monthly_data.copy()
        monthly_data_for_agent['Txn Date'] = monthly_data_for_agent['Txn Date'].dt.strftime('%Y-%m-%d')
        monthly_data_for_agent['month_year'] = monthly_data['month_year'].astype(str)
        
        # Check context window for monthly summary
        check_context_window_limit(user_info, monthly_data_for_agent, [], f"Financial Summary Agent - {month_year}")
        
        # Print monthly data size
        monthly_data_size = len(json.dumps(monthly_data_for_agent.to_dict('records')))
        print(f"Month {month_year} data size: {monthly_data_size:,} chars ({len(monthly_data_for_agent)} transactions)")
        
        summary = financial_summary_agent.summarize_user(user_info, monthly_data_for_agent)
        
        # Try to parse the summary as JSON if it's a string
        if isinstance(summary, str):
            try:
                summary_dict = json.loads(summary)
            except:
                summary_dict = {
                    "month": str(month_year).split('-')[1],
                    "year": str(month_year).split('-')[0],
                    "ai_summary": summary,
                    "categories_expenses": {}
                }
        else:
            summary_dict = summary
            
        monthly_summary.append(summary_dict)
    
    return monthly_summary


def get_email_notifications(user_info, recommendations, monthly_summary, product_data):
    """
    Generate email notifications
    
    Args:
        user_info (dict): User information
        recommendations (dict): Product recommendations
        monthly_summary (list): Monthly summaries
        product_data (dict): Product data
        
    Returns:
        dict: Email notification subjects
    """
    # Check context window for email notifications
    email_data = {
        "user_info": user_info,
        "recommendations": recommendations,
        "monthly_summary": monthly_summary
    }
    check_context_window_limit(email_data, [], product_data, "Email Notification Agent")
    
    # Generate email notifications
    email_notifications_result = email_notification_agent.run_email_notification_agent(
        user_info,
        recommendations,
        monthly_summary,
        product_data
    )
    
    # Parse email notifications
    try:
        email_subjects = json.loads(email_notifications_result) if isinstance(email_notifications_result, str) else email_notifications_result
    except:
        email_subjects = {
            "spending_summary_email": "Your Monthly Financial Insights Are Ready!",
            "coupons_email": "Great Savings Await You!",
            "loans_email": "Perfect Loan Options For You!",
            "credit_cards_email": "Amazing Credit Card Benefits!",
            "savings_email": "Grow Your Money Faster!"
        }
        
    return email_subjects


def process_user(user_id, user_info, transactions, product_data):
    """
    Process a single user
    
    Args:
        user_id (str): User ID
        user_info (dict): User information
        transactions (pd.DataFrame): Raw transaction data
        product_data (dict): Dictionary containing all product data
        
    Returns:
        dict: Final output data
    """
    print(f"\n===== Processing User {user_id} =====")
    
    # Step 1: Preprocess transactions
    transactions_processed = preprocess_transactions(transactions)
    
    # Convert datetime columns to strings for JSON serialization
    transactions_for_agents = transactions_processed.copy()
    transactions_for_agents['Txn Date'] = transactions_for_agents['Txn Date'].dt.strftime('%Y-%m-%d')
    transactions_for_agents['month_year'] = transactions_processed['month_year'].astype(str)
    
    # Step 2: Check context window limits
    print(f"\n===== Context Window Check for User {user_id} =====")
    for agent_name, product_list in [
        ("Coupons Agent", product_data['coupons']), 
        ("Loans Agent", product_data['loans']),
        ("Credit Cards Agent", product_data['credit_cards']), 
        ("Savings Agent", product_data['savings'])
    ]:
        check_context_window_limit(user_info, transactions_for_agents, product_list, agent_name)
    print("================================================\n")
    
    # Step 3: Get product recommendations
    print("Getting product recommendations...")
    recommendations = get_product_recommendations(user_info, transactions_for_agents, product_data)
    
    # Step 4: Generate monthly summaries
    print("Generating monthly summaries...")
    monthly_summary = generate_monthly_summaries(user_info, transactions_processed)
    
    # Step 5: Generate email notifications
    print("Generating email notifications...")
    email_subjects = get_email_notifications(user_info, recommendations, monthly_summary, product_data)
    
    # Step 6: Build final output
    final_output = build_final_output(
        user_info, 
        recommendations['coupons'], 
        recommendations['loans'], 
        recommendations['credit_cards'], 
        recommendations['high_yield_savings'], 
        monthly_summary,
        email_subjects
    )
    
    # Step 7: Save output to file
    with open(f'output/output_{user_id}.json', 'w') as f:
        json.dump(final_output, f, indent=2)
    
    print(f"Output saved for user {user_id}")
    return final_output


def run_pipeline():
    """
    Main pipeline function
    """
    # S3 configuration
    S3_BUCKET = "notifi-transaction-dataset"
    S3_PREFIX = "notifi-dump/"
    
    # Step 1: Get user IDs
    user_ids = get_user_ids()
    
    # Step 2: Load all data from S3
    print("Loading data from S3...")
    userinfo_df = read_csv_from_s3(S3_BUCKET, f"{S3_PREFIX}user.csv")
    
    # Load all product data
    product_data = {
        'coupons': read_csv_from_s3(S3_BUCKET, f"{S3_PREFIX}product_coupons_data.csv").to_dict(orient='records'),
        'loans': read_csv_from_s3(S3_BUCKET, f"{S3_PREFIX}loan_data.csv").to_dict(orient='records'),
        'credit_cards': read_csv_from_s3(S3_BUCKET, f"{S3_PREFIX}credit_card_data.csv").to_dict(orient='records'),
        'savings': read_csv_from_s3(S3_BUCKET, f"{S3_PREFIX}high_yield_savings_data.csv").to_dict(orient='records')
    }
    print("Data loaded successfully from S3!")

    # Step 3: Process each user
    for user_id in user_ids:
        # Print separator for clarity
        print(f"\n{'='*50}")
        print(f"Processing User {user_id}")
        print(f"{'='*50}")
        
        user_info = userinfo_df[userinfo_df['User_id'] == user_id].iloc[0].to_dict()
        transactions = get_user_transactions(user_id)
        process_user(user_id, user_info, transactions, product_data)


if __name__ == "__main__":
    run_pipeline()