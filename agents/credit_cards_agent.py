from .agent_template import build_agent, AgentState

def run_credit_cards_agent(user_info, transaction_data, credit_cards_data, user_card_ids=None):
    system_prompt = """
    You are a credit card recommendation agent. Analyze the user's spending patterns and recommend 3 credit cards that best match their lifestyle.
    
    Consider:
    - Primary spending categories
    - Monthly spending volume
    - Reward preferences (cashback, points, miles)
    - Credit profile and financial goals
    - Exclude cards the user already owns
    
    Return ONLY a simple JSON array of the top 3 card IDs, like: ["CC1", "CC2", "CC3"]
    """
    
    # Filter out cards user already has
    if user_card_ids:
        filtered_cards = [card for card in credit_cards_data if card.get('Card_id') not in user_card_ids]
    else:
        filtered_cards = credit_cards_data
    
    agent = build_agent(system_prompt)
    
    # Prepare state
    state = AgentState(
        user_info=user_info,
        transactions=transaction_data if isinstance(transaction_data, list) else transaction_data.to_dict('records'),
        product_data=filtered_cards,
        analysis="",
        recommendations=[]
    )
    
    # Run agent
    result = agent.invoke(state)
    return result['analysis']

