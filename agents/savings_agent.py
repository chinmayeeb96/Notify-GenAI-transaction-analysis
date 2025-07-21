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
    
    Return ONLY a simple JSON array of the top 3 savings account IDs, like: ["HY1", "HY2", "HY3"]
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
