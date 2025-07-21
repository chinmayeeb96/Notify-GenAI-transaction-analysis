#!/usr/bin/env python3
"""
Test script for the enhanced email notification agent
"""
import sys
import os
sys.path.append('.')
import json
from agents import email_notification_agent

def test_email_notifications():
    """Test the email notification agent with sample data"""
    
    # Sample user info
    user_info = {
        "User_id": "U1",
        "User_name": "John Smith", 
        "Credit_score": 720,
        "Age": 45,
        "Financial_goals": "Save for emergency fund",
        "Gender": "Male",
        "Email": "john.smith@email.com"
    }
    
    # Sample product recommendations (top choices)
    product_recommendations = {
        "coupons": ["CO4", "CO1", "CO2"],
        "loans": ["LN1", "LN4", "LN5"], 
        "credit_cards": ["CC1", "CC2", "CC3"],
        "high_yield_savings": ["HY1", "HY3", "HY5"]
    }
    
    # Sample monthly summaries
    monthly_summaries = [
        {
            "month": "04",
            "year": "2023",
            "ai_summary": "In April 2023, your total income was $3,500, and your total spending was $2,097.96, which is around 60% of your income. Your major expenses were rent ($1,500), electronics ($289.99), clothing ($120), and food ($80). While entertainment expenses were relatively low, you could consider reducing discretionary spending on electronics and clothing to allocate more funds towards your emergency fund goal.",
            "categories_expenses": {
                "total_income": "-3500.0",
                "food": "80.0",
                "food_%": "2.29%",
                "transportation": "0.0", 
                "transportation_%": "0.0%",
                "entertainment": "27.98",
                "entertainment_%": "0.8%",
                "rent": "1500.0",
                "rent_%": "42.86%",
                "personal_care": "60.0",
                "personal_care_%": "1.71%",
                "general_merchandise": "409.99", 
                "general_merchandise_%": "11.71%",
                "total_spending": "2097.96",
                "total_spending_%": "59.94%"
            }
        }
    ]
    
    # Sample product data
    sample_product_data = {
        "coupons": [
            {
                "coupon_id": "CO4",
                "coupon_code": "BOGO23TECH",
                "discount_percentage": "BOGO",
                "minimum_purchase": "$75.00",
                "expiry_date": "2023-09-25", 
                "product_category": "Electronics",
                "merchant_name": "Best Buy"
            }
        ],
        "loans": [
            {
                "loan_id": "LN1",
                "loan_type": "Personal Loan",
                "interest_rate_range": "6.99% - 19.99%", 
                "term_length": "1-5 years",
                "minimum_amount": "$3000",
                "maximum_amount": "$35000",
                "required_credit_score": "680+",
                "bank_name": "Chase"
            }
        ],
        "credit_cards": [
            {
                "card_id": "CC1", 
                "card_name": "Chase Sapphire Preferred",
                "annual_fee": "$95",
                "rewards_rate": "2X points on travel, 3X on dining, 1X on all other purchases",
                "welcome_bonus": "60,000 bonus points after spending $4,000 in first 3 months",
                "issuer": "Chase"
            }
        ],
        "savings": [
            {
                "id": "HY1",
                "account_name": "High-Yield Savings",
                "apy": "4.50%",
                "bank_name": "Marcus by Goldman Sachs",
                "minimum_balance": "$0"
            }
        ]
    }
    
    try:
        print("Testing Enhanced Email Notification Agent...")
        print("=" * 50)
        
        # Run the email notification agent
        email_notifications = email_notification_agent.run_email_notification_agent(
            user_info,
            product_recommendations, 
            monthly_summaries,
            sample_product_data
        )
        
        print("✅ Email notifications generated successfully!")
        print()
        print(f"User: {user_info['User_name']}")
        print(f"Goal: {user_info['Financial_goals']}")
        print(f"Credit Score: {user_info['Credit_score']}")
        print()
        print("Enhanced Email Notifications:")
        print("-" * 30)
        
        # Parse the result if it's a string
        if isinstance(email_notifications, str):
            try:
                email_notifications = json.loads(email_notifications)
            except:
                print("Raw response:", email_notifications)
                return
        
        # Display results
        for key, value in email_notifications.items():
            print(f"{key.replace('_', ' ').title()}: {value}")
            
        return email_notifications
        
    except Exception as e:
        print(f"❌ Error testing email notifications: {e}")
        import traceback
        traceback.print_exc()
        return None

if __name__ == "__main__":
    test_email_notifications()
