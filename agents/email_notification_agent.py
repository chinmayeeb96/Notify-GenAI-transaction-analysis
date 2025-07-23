from .agent_template import build_agent, AgentState
from .email_notification_agent_prompts.v6 import system_prompt

def generate_email_notifications(user_info, top_recommendations, monthly_summaries, product_data):
    """
    Generate creative email notifications for all product recommendations and monthly summary
    
    Args:
        user_info (dict): User information
        top_recommendations (dict): Top recommendations from each product agent
        monthly_summaries (list): Monthly financial analysis data
        product_data (dict): All product data for context lookup
    
    Returns:
        dict: Creative email subjects for each category
    """
    
    agent = build_agent(system_prompt)
    
    # Prepare comprehensive context for the agent with enhanced details
    spending_insights = extract_spending_insights(monthly_summaries)
    user_financial_profile = create_user_financial_profile(user_info, monthly_summaries)
    
    context_data = {
        "user_info": user_info,
        "user_financial_profile": user_financial_profile,
        "spending_insights": spending_insights,
        "top_recommendations": top_recommendations,
        "monthly_summaries": monthly_summaries,
        "product_details": product_data,
        "personalization_context": {
            "first_name": user_info.get('User_name', '').split(' ')[0],
            "credit_score": user_info.get('Credit_score', 0),
            "financial_goals": user_info.get('Financial_goals', ''),
            "age": user_info.get('Age', 0)
        }
    }
    
    # Prepare state
    state = AgentState(
        user_info=user_info,
        transactions=[],  # Not needed for email generation
        product_data=[context_data],  # Pass all context as product_data
        analysis="",
        recommendations=[]
    )
    
    # Run agent
    result = agent.invoke(state)
    return result['analysis']


def get_top_product_details(product_id, product_list, product_type):
    """
    Helper function to get enhanced details of a specific product by ID
    
    Args:
        product_id (str): Product ID to lookup
        product_list (list): List of products 
        product_type (str): Type of product (for ID field mapping)
    
    Returns:
        dict: Enhanced product details with extracted key features
    """
    id_field_map = {
        'coupons': 'coupon_id',
        'loans': 'loan_id', 
        'credit_cards': 'card_id',
        'savings': 'id'
    }
    
    id_field = id_field_map.get(product_type, 'id')
    
    for product in product_list:
        if product.get(id_field) == product_id:
            enhanced_product = product.copy()
            
            # Add extracted key features based on product type
            if product_type == 'coupons':
                enhanced_product['key_feature'] = f"{product.get('discount_percentage', '')} off at {product.get('merchant_name', '')}"
                enhanced_product['urgency'] = product.get('expiry_date', '')
                
            elif product_type == 'loans':
                rate_range = product.get('interest_rate_range', '')
                enhanced_product['key_feature'] = f"{rate_range} APR from {product.get('bank_name', '')}"
                enhanced_product['loan_range'] = f"${product.get('minimum_amount', '')}-${product.get('maximum_amount', '')}"
                
            elif product_type == 'credit_cards':
                welcome_bonus = product.get('welcome_bonus', '')
                rewards_rate = product.get('rewards_rate', '')
                enhanced_product['key_feature'] = welcome_bonus or rewards_rate
                enhanced_product['issuer'] = product.get('issuer', '')
                enhanced_product['annual_fee'] = product.get('annual_fee', '$0')
                
            elif product_type == 'savings':
                apy = product.get('apy', product.get('interest_rate', ''))
                bank = product.get('bank_name', product.get('institution', ''))
                enhanced_product['key_feature'] = f"{apy} APY at {bank}"
                enhanced_product['minimum'] = product.get('minimum_balance', '$0')
            
            return enhanced_product
    
    return {}


def run_email_notification_agent(user_info, product_recommendations, monthly_summaries, all_product_data):
    """
    Main function to run the email notification agent
    
    Args:
        user_info (dict): User information
        product_recommendations (dict): Top recommendations from each agent
        monthly_summaries (list): Monthly financial summaries  
        all_product_data (dict): All product data for context lookup
    
    Returns:
        dict: Email notification subjects
    """
    # Extract top recommendations (first item from each list)
    top_recommendations = {}
    for product_type, rec_list in product_recommendations.items():
        if rec_list and len(rec_list) > 0:
            top_recommendations[product_type] = rec_list[0]
    
    # Get detailed info for each top recommendation
    product_details = {}
    
    if 'coupons' in top_recommendations:
        product_details['top_coupon'] = get_top_product_details(
            top_recommendations['coupons'], 
            all_product_data.get('coupons', []), 
            'coupons'
        )
    
    if 'loans' in top_recommendations:
        product_details['top_loan'] = get_top_product_details(
            top_recommendations['loans'],
            all_product_data.get('loans', []),
            'loans'
        )
        
    if 'credit_cards' in top_recommendations:
        product_details['top_credit_card'] = get_top_product_details(
            top_recommendations['credit_cards'],
            all_product_data.get('credit_cards', []),
            'credit_cards'
        )
        
    if 'high_yield_savings' in top_recommendations:
        product_details['top_savings'] = get_top_product_details(
            top_recommendations['high_yield_savings'],
            all_product_data.get('savings', []),
            'savings'
        )
    
    # Run the notification agent
    email_notifications = generate_email_notifications(
        user_info, 
        top_recommendations,
        monthly_summaries,
        product_details
    )
    
    return email_notifications


def extract_spending_insights(monthly_summaries):
    """
    Extract key spending insights and patterns from monthly summaries
    
    Args:
        monthly_summaries (list): List of monthly financial data
    
    Returns:
        dict: Key spending insights and trends
    """
    if not monthly_summaries:
        return {}
    
    latest_month = monthly_summaries[-1] if monthly_summaries else {}
    
    # Safe conversion functions
    def safe_float(value):
        try:
            if isinstance(value, str):
                # Remove currency symbols and commas
                value = value.replace('$', '').replace(',', '').replace('"', '')
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def safe_abs_float(value):
        return abs(safe_float(value))
    
    insights = {
        "latest_month_year": f"{latest_month.get('month', '')}/{latest_month.get('year', '')}",
        "total_spending": safe_float(latest_month.get('categories_expenses', {}).get('total_spending', 0)),
        "total_income": safe_abs_float(latest_month.get('categories_expenses', {}).get('total_income', 0)),
        "spending_ratio": safe_float(latest_month.get('categories_expenses', {}).get('total_spending_%', 0)),
        "top_categories": [],
        "savings_potential": 0,
        "key_highlights": []
    }
    
    # Extract top spending categories
    categories = latest_month.get('categories_expenses', {})
    category_amounts = {}
    for key, value in categories.items():
        if key.endswith('_%') or key.startswith('total_'):
            continue
        try:
            numeric_value = safe_float(value)
            if numeric_value > 0:
                category_amounts[key] = numeric_value
        except:
            continue
    
    # Sort categories by amount
    sorted_categories = sorted(category_amounts.items(), key=lambda x: x[1], reverse=True)[:3]
    insights["top_categories"] = sorted_categories
    
    # Calculate savings potential (income - spending)
    insights["savings_potential"] = insights["total_income"] - insights["total_spending"]
    
    # Extract AI summary highlights
    ai_summary = latest_month.get('ai_summary', '')
    if ai_summary:
        # Extract key numbers and percentages
        import re
        percentages = re.findall(r'(\d+\.?\d*)%', ai_summary)
        dollar_amounts = re.findall(r'\$(\d+[,\d]*\.?\d*)', ai_summary)
        
        if percentages:
            insights["key_highlights"].append(f"spending at {percentages[0]}% of income")
        if dollar_amounts:
            insights["key_highlights"].append(f"${dollar_amounts[0]} in expenses")
    
    return insights


def create_user_financial_profile(user_info, monthly_summaries):
    """
    Create a comprehensive financial profile for personalized recommendations
    
    Args:
        user_info (dict): User information
        monthly_summaries (list): Monthly spending data
    
    Returns:
        dict: User financial profile for personalization
    """
    profile = {
        "credit_tier": "good",
        "life_stage": "working_professional",
        "spending_style": "balanced",
        "savings_priority": "medium",
        "risk_tolerance": "moderate"
    }
    
    credit_score = user_info.get('Credit_score', 650)
    age = user_info.get('Age', 35)
    financial_goals = user_info.get('Financial_goals', '').lower()
    
    # Determine credit tier
    if credit_score >= 750:
        profile["credit_tier"] = "excellent"
    elif credit_score >= 700:
        profile["credit_tier"] = "good"  
    elif credit_score >= 650:
        profile["credit_tier"] = "fair"
    else:
        profile["credit_tier"] = "poor"
    
    # Determine life stage
    if age < 30:
        profile["life_stage"] = "young_professional"
    elif age < 45:
        profile["life_stage"] = "working_professional"
    elif age < 65:
        profile["life_stage"] = "pre_retirement"
    else:
        profile["life_stage"] = "retirement"
    
    # Analyze financial goals
    if "emergency" in financial_goals or "save" in financial_goals:
        profile["savings_priority"] = "high"
    elif "investment" in financial_goals or "retirement" in financial_goals:
        profile["risk_tolerance"] = "aggressive"
    elif "debt" in financial_goals or "pay" in financial_goals:
        profile["savings_priority"] = "debt_focused"
    
    # Analyze spending patterns if available
    if monthly_summaries:
        latest = monthly_summaries[-1].get('categories_expenses', {})
        spending_ratio_raw = latest.get('total_spending_%', 0)
        
        try:
            # Handle string percentages like "59.94%"
            if isinstance(spending_ratio_raw, str):
                spending_ratio_raw = spending_ratio_raw.replace('%', '').replace('"', '')
            spending_ratio = float(spending_ratio_raw)
        except (ValueError, TypeError):
            spending_ratio = 0
        
        if spending_ratio > 80:
            profile["spending_style"] = "high_spender"
        elif spending_ratio < 50:
            profile["spending_style"] = "saver"
        else:
            profile["spending_style"] = "balanced"
    
    return profile
