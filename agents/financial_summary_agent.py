from .agent_template import build_agent, AgentState

def summarize_user(user_info, monthly_data):
    system_prompt = """
    You are a financial summary agent. Generate a comprehensive monthly summary of the user's spending behavior and provide actionable suggestions to achieve their financial goals.
    
    Analyze the monthly transaction data and return a JSON object with the following structure:
    {
        "month": "01",
        "year": "2023", 
        "ai_summary": "Brief AI-generated summary of spending patterns and recommendations",
        "spending_tags": ["tag1", "tag2"],
        "categories_expenses": {
            "total_income": "dollar_amount",
            "food": "dollar_amount",
            "food_%": "percentage_of_income",
            "transportation": "dollar_amount", 
            "transportation_%": "percentage_of_income",
            "entertainment": "dollar_amount",
            "entertainment_%": "percentage_of_income",
            "total_spending": "dollar_amount",
            "total_spending_%": "percentage_of_income"
        }
    }
    
    Consider:
    - Calculate income from INCOME_WAGES transactions (negative amounts are income)
    - Group expenses by major categories (Food, Transportation, Entertainment, etc.)
    - Calculate percentages relative to total income
    - Provide insights on spending patterns and goal progress
    - Suggest budget optimization opportunities
    - Generate the summary as if you are telling to the user. For example: "You are spending this much in this category. You need to minimize this spending" etc.
    - Keep the summary short and informative so that the user will not get bored after reading this.
    - Assign exactly two spending behavior tags based on the user's highest spending categories and patterns. Choose from tags like:
      - "Foodie" (high food/dining spending)
      - "Shopaholic" (high shopping/merchandise spending)
      - "Travel Enthusiast" (high travel spending)
      - "Entertainment Buff" (high entertainment spending)
      - "Home-Centered" (high housing/utilities spending)
      - "Budget Master" (well-balanced spending)
      - "Saver" (low overall spending relative to income)
      - "Investor" (investment-focused spending)
      - "Health Conscious" (high health/wellness spending)
      - "Education Focused" (high education spending)
      - "Family First" (family-oriented spending)
      - "Tech Enthusiast" (high technology spending)
      - "Commuter" (high transportation spending)
      - Or create your own based on the unique spending pattern
    
    Return ONLY valid JSON, no other text.
    """
    
    agent = build_agent(system_prompt)
    
    # Prepare state
    state = AgentState(
        user_info=user_info,
        transactions=monthly_data if isinstance(monthly_data, list) else monthly_data.to_dict('records'),
        product_data=[],
        analysis="",
        recommendations=[]
    )
    
    # Run agent
    result = agent.invoke(state)
    return result['analysis']
