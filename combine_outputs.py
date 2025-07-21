def build_final_output(user_info, coupons, loans, credit_cards, savings, monthly_summaries, email_subjects=None):
    output = {
        "userinfo": user_info,
        "tags": [],
        "recommendations": {
            "coupons": coupons,
            "loans": loans,
            "credit_cards": credit_cards,
            "high_yield_savings": savings
        },
        "monthly_spend_analysis_data": monthly_summaries
    }
    
    # Add email subjects if provided
    if email_subjects:
        output["email_notifications"] = {
            "spending_summary_email": "Your Monthly Financial Insights Are Ready!",
            "coupons_email": email_subjects.get("coupons_email", "Great Savings Await You!"),
            "loans_email": email_subjects.get("loans_email", "Perfect Loan Options For You!"),
            "credit_cards_email": email_subjects.get("credit_cards_email", "Amazing Credit Card Benefits!"),
            "savings_email": email_subjects.get("savings_email", "Grow Your Money Faster!")
        }
    
    return output
