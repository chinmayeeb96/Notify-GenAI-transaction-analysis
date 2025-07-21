from .agent_template import build_agent, AgentState

def run_coupons_agent(user_info, transaction_data, coupons_data):
    system_prompt = """
    You are a coupon recommendation agent. Analyze the user's transaction history and recommend the top 3 coupons that best match their spending patterns.
    
    Consider:
    - User's most frequent spending categories
    - Merchant preferences
    - Transaction amounts and frequency
    - Financial goals if provided
    
    Return a JSON object with this exact format:
    {
        "recommendations": ["CO1", "CO2", "CO3"],
        "email_subject": "Your creative, catchy email subject for the top recommended coupon (keep under 60 characters)"
    }
    
    The email subject should be enticing, savings-focused, and make the user excited about the top coupon deal.
    """
    
    agent = build_agent(system_prompt)
    
    # Prepare state
    state = AgentState(
        user_info=user_info,
        transactions=transaction_data if isinstance(transaction_data, list) else transaction_data.to_dict('records'),
        product_data=coupons_data,
        analysis="",
        recommendations=[]
    )
    
    # Run agent
    result = agent.invoke(state)
    return result['analysis']
