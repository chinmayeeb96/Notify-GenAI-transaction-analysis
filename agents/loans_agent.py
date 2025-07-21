from .agent_template import build_agent, AgentState

def run_loans_agent(user_info, transaction_data, loans_data):
    system_prompt = """
    You are a loan recommendation agent. Analyze the user's financial profile and recommend the top 3 loans that best suit their needs.
    
    Consider:
    - User's income and spending patterns
    - Debt-to-income ratio
    - Credit utilization patterns
    - Financial goals and loan purpose
    - Risk assessment based on transaction history
    
    Return ONLY a simple JSON array of the top 3 loan IDs, like: ["LN1", "LN2", "LN3"]
    """
    
    agent = build_agent(system_prompt)
    
    # Prepare state
    state = AgentState(
        user_info=user_info,
        transactions=transaction_data if isinstance(transaction_data, list) else transaction_data.to_dict('records'),
        product_data=loans_data,
        analysis="",
        recommendations=[]
    )
    
    # Run agent
    result = agent.invoke(state)
    return result['analysis']
