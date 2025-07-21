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


def run_pipeline():
    # S3 configuration
    S3_BUCKET = "notifi-transaction-dataset"
    S3_PREFIX = "notifi-dump/"
    
    user_ids = get_user_ids()
    
    # Read data from S3 instead of local files
    print("Loading data from S3...")
    userinfo_df = read_csv_from_s3(S3_BUCKET, f"{S3_PREFIX}user.csv")
    coupons = read_csv_from_s3(S3_BUCKET, f"{S3_PREFIX}product_coupons_data.csv").to_dict(orient='records')
    loans = read_csv_from_s3(S3_BUCKET, f"{S3_PREFIX}loan_data.csv").to_dict(orient='records')
    credit_cards = read_csv_from_s3(S3_BUCKET, f"{S3_PREFIX}credit_card_data.csv").to_dict(orient='records')
    savings = read_csv_from_s3(S3_BUCKET, f"{S3_PREFIX}high_yield_savings_data.csv").to_dict(orient='records')
    print("Data loaded successfully from S3!")

    for user_id in user_ids:
        user_info = userinfo_df[userinfo_df['User_id'] == user_id].iloc[0].to_dict()
        transactions = get_user_transactions(user_id)
        
        # Convert Txn Date to datetime and extract month-year
        transactions['Txn Date'] = pd.to_datetime(transactions['Txn Date'])
        transactions['month_year'] = transactions['Txn Date'].dt.to_period('M')
        
        # Convert datetime columns to strings for JSON serialization
        transactions_for_agents = transactions.copy()
        transactions_for_agents['Txn Date'] = transactions_for_agents['Txn Date'].dt.strftime('%Y-%m-%d')
        transactions_for_agents['month_year'] = transactions_for_agents['month_year'].astype(str)

        coupons_rec = coupons_agent.run_coupons_agent(user_info, transactions_for_agents, coupons)
        loans_rec = loans_agent.run_loans_agent(user_info, transactions_for_agents, loans)
        credit_rec = credit_cards_agent.run_credit_cards_agent(user_info, transactions_for_agents, credit_cards)
        savings_rec = savings_agent.run_savings_agent(user_info, transactions_for_agents, savings)
        
        # Parse JSON responses or use fallback - now expecting simple arrays
        def get_full_data(recommendations, all_data, id_key):
            if not isinstance(recommendations, list):
                recommendations = []
            # Compare using the value of the first column (assumed to be the id)
            return [item for item in all_data if str(list(item.values())[0]) in [str(r) for r in recommendations]]

        try:
            coupons_recommendations_ids = json.loads(coupons_rec) if isinstance(coupons_rec, str) else coupons_rec
            if not isinstance(coupons_recommendations_ids, list):
                coupons_recommendations_ids = ["CO1", "CO2", "CO3"]
        except:
            coupons_recommendations_ids = ["CO1", "CO2", "CO3"]
        coupons_recommendations = get_full_data(coupons_recommendations_ids, coupons, "Coupon_id")

        try:
            loans_recommendations_ids = json.loads(loans_rec) if isinstance(loans_rec, str) else loans_rec
            if not isinstance(loans_recommendations_ids, list):
                loans_recommendations_ids = ["LN1", "LN2", "LN3"]
        except:
            loans_recommendations_ids = ["LN1", "LN2", "LN3"]
        loans_recommendations = get_full_data(loans_recommendations_ids, loans, "Loan_id")

        try:
            credit_recommendations_ids = json.loads(credit_rec) if isinstance(credit_rec, str) else credit_rec
            if not isinstance(credit_recommendations_ids, list):
                credit_recommendations_ids = ["CC1", "CC2", "CC3"]
        except:
            credit_recommendations_ids = ["CC1", "CC2", "CC3"]
        credit_recommendations = get_full_data(credit_recommendations_ids, credit_cards, "CreditCard_id")

        try:
            savings_recommendations_ids = json.loads(savings_rec) if isinstance(savings_rec, str) else savings_rec
            if not isinstance(savings_recommendations_ids, list):
                savings_recommendations_ids = ["HY1", "HY2", "HY3"]
        except:
            savings_recommendations_ids = ["HY1", "HY2", "HY3"]
        savings_recommendations = get_full_data(savings_recommendations_ids, savings, "Savings_id")
        
        print("got the recommendations")
        
        monthly_summary = []
        # Group transactions by month and create monthly summaries
        for month_year in transactions['month_year'].unique():
            monthly_data = transactions[transactions['month_year'] == month_year].copy()
            
            # Convert datetime columns to strings for the agent
            monthly_data_for_agent = monthly_data.copy()
            monthly_data_for_agent['Txn Date'] = monthly_data_for_agent['Txn Date'].dt.strftime('%Y-%m-%d')
            monthly_data_for_agent['month_year'] = monthly_data_for_agent['month_year'].astype(str)
            
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

        # Generate creative email notifications using the dedicated agent
        product_recommendations_dict = {
            "coupons": coupons_recommendations,
            "loans": loans_recommendations, 
            "credit_cards": credit_recommendations,
            "high_yield_savings": savings_recommendations
        }
        
        all_product_data = {
            "coupons": coupons,
            "loans": loans,
            "credit_cards": credit_cards,
            "savings": savings
        }
        
        email_notifications_result = email_notification_agent.run_email_notification_agent(
            user_info,
            product_recommendations_dict,
            monthly_summary,
            all_product_data
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

        final_output = build_final_output(
            user_info, 
            coupons_recommendations, 
            loans_recommendations, 
            credit_recommendations, 
            savings_recommendations, 
            monthly_summary,
            email_subjects
        )
        
        
        # Save output to file
        with open(f'output/output_{user_id}.json', 'w') as f:
            json.dump(final_output, f, indent=2)
        
        print(f"Output saved for user {user_id}")


if __name__ == "__main__":
    run_pipeline()