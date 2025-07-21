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
        model_id='anthropic.claude-3-sonnet-20240229-v1:0',
        model_kwargs={
            "max_tokens": 4000,
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