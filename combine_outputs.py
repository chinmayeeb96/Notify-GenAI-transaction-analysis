def build_final_output(user_info, coupons, loans, credit_cards, savings, monthly_summaries, email_subjects=None, product_data=None):
    # Analyze spending tags across all months to find most representative tags
    tag_frequency = {}
    
    # Count frequency of each tag across all months
    for summary in monthly_summaries:
        if "spending_tags" in summary and isinstance(summary["spending_tags"], list):
            for tag in summary["spending_tags"]:
                if tag in tag_frequency:
                    tag_frequency[tag] += 1
                else:
                    tag_frequency[tag] = 1
    
    # Sort tags by frequency
    sorted_tags = sorted(tag_frequency.items(), key=lambda x: x[1], reverse=True)
    
    # Get the top 2 most frequent tags
    spending_tags = [tag for tag, freq in sorted_tags[:2]] if sorted_tags else []
    
    # If we don't have 2 tags yet and there's at least one monthly summary
    if len(spending_tags) < 2 and monthly_summaries and len(monthly_summaries) > 0:
        # Try to get tags from the most recent month
        latest_summary = monthly_summaries[-1]
        if "spending_tags" in latest_summary and isinstance(latest_summary["spending_tags"], list):
            # Add any missing tags from the latest month
            for tag in latest_summary["spending_tags"]:
                if tag not in spending_tags:
                    spending_tags.append(tag)
                    if len(spending_tags) >= 2:
                        break

    output = {
        "userinfo": user_info,
        "tags": spending_tags,
        "recommendations": {}
    }
    
    # Map product IDs to full details if product_data is provided
    if product_data:
        # Map coupons to full details
        coupon_details = []
        for coupon_id in coupons:
            coupon_info = next((c for c in product_data.get('coupons', []) if c.get('coupon_id') == coupon_id), None)
            if coupon_info:
                coupon_details.append(coupon_info)
            else:
                # If coupon not found, include only the ID
                coupon_details.append({"Coupon_id": coupon_id})
        
        # Map loans to full details
        loan_details = []
        for loan_id in loans:
            loan_info = next((l for l in product_data.get('loans', []) if l.get('loan_id') == loan_id), None)
            if loan_info:
                loan_details.append(loan_info)
            else:
                # If loan not found, include only the ID
                loan_details.append({"Loan_id": loan_id})
        
        # Map credit cards to full details
        credit_card_details = []
        for cc_id in credit_cards:
            cc_info = next((c for c in product_data.get('credit_cards', []) if c.get('card_id') == cc_id), None)
            if cc_info:
                credit_card_details.append(cc_info)
            else:
                # If credit card not found, include only the ID
                credit_card_details.append({"Credit_card_id": cc_id})
        
        # Map savings accounts to full details
        savings_details = []
        for savings_id in savings:
            savings_info = next((s for s in product_data.get('savings', []) if s.get('id') == savings_id), None)
            if savings_info:
                savings_details.append(savings_info)
            else:
                # If savings account not found, include only the ID
                savings_details.append({"High_yield_savings_id": savings_id})
        
        # Add full details to output
        output["recommendations"] = {
            "coupons": coupon_details,
            "loans": loan_details,
            "credit_cards": credit_card_details,
            "high_yield_savings": savings_details
        }
    else:
        # If product_data is not provided, just use the IDs
        output["recommendations"] = {
            "coupons": coupons,
            "loans": loans,
            "credit_cards": credit_cards,
            "high_yield_savings": savings
        }
    
    # Add monthly summaries to output
    output["monthly_spend_analysis_data"] = monthly_summaries
    
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
