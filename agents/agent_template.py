import boto3
from langchain_aws import ChatBedrock
from langgraph.graph import StateGraph, END
from typing import TypedDict, List
import json


class AgentState(TypedDict):
    user_info: dict
    transactions: List[dict]
    product_data: List[dict]
    analysis: str
    recommendations: List[dict]


def build_agent(system_prompt):
    """
    Build a LangGraph agent using AWS Bedrock Claude model
    """
    # Initialize Bedrock client using AWS CLI credentials
    bedrock_client = boto3.client(
        service_name='bedrock-runtime',
        region_name='us-east-1'
    )

    # Create ChatBedrock instance
    llm = ChatBedrock(
        client=bedrock_client,
        model_id='anthropic.claude-3-5-sonnet-20240620-v1:0',
        model_kwargs={
            "max_tokens": 100000,
            "temperature": 0.1,
            "system": system_prompt
        }
    )
    
    def analyze_data(state: AgentState):
        """Analyze user data and generate recommendations"""
        try:
            # Convert any datetime objects to strings for JSON serialization
            def serialize_data(data):
                if isinstance(data, list):
                    return [serialize_data(item) for item in data]
                elif isinstance(data, dict):
                    return {key: serialize_data(value) for key, value in data.items()}
                elif hasattr(data, 'isoformat'):  # datetime objects
                    return data.isoformat()
                elif hasattr(data, '__str__') and 'Timestamp' in str(type(data)):  # pandas Timestamp
                    return str(data)
                else:
                    return data
            
            # Serialize the data
            serialized_user_info = serialize_data(state['user_info'])
            serialized_transactions = serialize_data(state['transactions'][:10])
            serialized_products = serialize_data(state['product_data'][:5])
            
            # Prepare input for the LLM
            user_context = f"""
            User Information: {json.dumps(serialized_user_info, indent=2)}
            Transaction Data: {json.dumps(serialized_transactions, indent=2)}
            Available Products: {json.dumps(serialized_products, indent=2)}
            
            Please analyze the user's financial behavior and recommend suitable products.
            """
            
            # Get response from Claude
            response = llm.invoke(user_context)
            
            # Update state with analysis
            state['analysis'] = response.content
            
            return state
            
        except Exception as e:
            state['analysis'] = f"Error in analysis: {str(e)}"
            return state
    
    # Create the agent graph
    workflow = StateGraph(AgentState)
    
    # Add nodes
    workflow.add_node("analyze", analyze_data)
    
    # Set entry point
    workflow.set_entry_point("analyze")
    
    # Add edges
    workflow.add_edge("analyze", END)
    
    # Compile the graph
    agent = workflow.compile()
    
    return agent


def estimate_token_size(data):
    """
    Roughly estimate the token size of data being sent to the LLM
    
    Args:
        data: Any Python object (dict, list, etc.)
        
    Returns:
        int: Estimated token count
    """
    # Convert to JSON string to simulate what would be sent to the LLM
    json_str = json.dumps(data, default=str)
    
    # A rough approximation: 1 token ≈ 4 characters for English text
    # This is just an estimate - actual tokenization depends on the model
    char_count = len(json_str)
    estimated_tokens = char_count / 4
    
    return int(estimated_tokens)


def check_context_window_limit(user_info, transactions, product_data, agent_name, max_tokens=100000):
    """
    Check if data might exceed LLM context window
    
    Args:
        user_info: User information dictionary
        transactions: Transaction data (DataFrame or list)
        product_data: Product data
        agent_name: Name of the agent being called
        max_tokens: Maximum token limit to warn about
        
    Returns:
        bool: True if likely to exceed limit, False otherwise
    """
    # Convert transactions to list if it's a DataFrame
    if hasattr(transactions, 'to_dict'):
        transactions_list = transactions.to_dict('records')
    else:
        transactions_list = transactions
    
    # Estimate token sizes
    user_info_tokens = estimate_token_size(user_info)
    transactions_tokens = estimate_token_size(transactions_list)
    product_data_tokens = estimate_token_size(product_data)
    
    # Total estimated tokens
    total_tokens = user_info_tokens + transactions_tokens + product_data_tokens
    print("Total estimated tokens:", total_tokens)
    # Account for prompt, model instructions, etc.
    total_tokens += 1000  # Add buffer for system prompt, etc.
    
    # Print warning if close to or exceeding limit
    if total_tokens > max_tokens * 0.8:
        print(f"⚠️ WARNING: {agent_name} might exceed context window!")
        print(f"  Estimated tokens: {total_tokens:,}")
        print(f"  User info: {user_info_tokens:,} tokens")
        print(f"  Transactions: {transactions_tokens:,} tokens ({len(transactions_list)} records)")
        print(f"  Product data: {product_data_tokens:,} tokens")
        return True
    
    return False