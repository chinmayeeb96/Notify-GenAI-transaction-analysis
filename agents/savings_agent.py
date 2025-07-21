from .agent_template import build_agent, AgentState

def run_savings_agent(user_info, transaction_data, savings_data):
    system_prompt = """
    You are a high-yield savings account recommendation agent. Analyze the user's financial behavior and recommend suitable savings options.
    
    Consider:
    - User's income and spending patterns
    - Savings potential based on transaction history
    - Current savings goals and financial objectives
    - Risk tolerance and liquidity needs
    - Interest rates and account features
    
    Return a JSON object with this exact format:
    {
        "recommendations": ["HY1", "HY2", "HY3"],
        "email_subject": "Your creative, catchy email subject for the top recommended product (keep under 60 characters)"
    }
    
    The email subject should be attention-grabbing, personalized, and make the user want to open the email about the top savings account.
    """
    
    agent = build_agent(system_prompt)
    
    # Prepare state
    state = AgentState(
        user_info=user_info,
        transactions=transaction_data if isinstance(transaction_data, list) else transaction_data.to_dict('records'),
        product_data=savings_data,
        analysis="",
        recommendations=[]
    )
    
    # Run agent
    result = agent.invoke(state)
    return result['analysis']
