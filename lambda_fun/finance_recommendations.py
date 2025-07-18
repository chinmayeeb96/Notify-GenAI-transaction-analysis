import uuid
from datetime import datetime
from typing import Dict, Any, List

from s3_manager import S3Manager
from db_manager import DatabaseManager
from prompt_builder import PromptBuilder
from llm_client import LLMClient
from models import Recommendation
from config import TRANSACTIONS_PREFIX

class FinanceRecommender:
    def __init__(self):
        self.s3_manager = S3Manager()
        self.db_manager = DatabaseManager()
        self.llm_client = LLMClient()

    def process_user_recommendations(self, file_key: str) -> Dict[int, List[str]]:
        """
        Process recommendations for users based on their transaction history
        
        Args:
            file_key (str): S3 key for the transactions CSV file
            
        Returns:
            Dict[int, List[str]]: Dictionary mapping user IDs to their recommended coupons
        """
        # Step 1: Read transactions from S3
        transactions = self.s3_manager.read_transactions_csv(file_key)
        
        # Step 2: Get unique user IDs
        user_ids = self.s3_manager.get_unique_user_ids(transactions)
        
        # Step 3: Get user information from Aurora
        users = self.db_manager.get_users_info(user_ids)
        
        # Step 4: Get all available coupons
        coupons = self.db_manager.get_all_coupons()
        
        # Step 5: Process each user
        recommendations_dict = {}
        recommendations_to_store = []
        
        for user_id in user_ids:
            if user_id not in users:
                continue
                
            user = users[user_id]
            user_transactions = [t for t in transactions if t.user_id == user_id]
            
            # Create prompt for the user
            prompt = PromptBuilder.create_recommendation_prompt(
                user=user,
                transactions=user_transactions,
                coupons=coupons
            )
            
            # Get recommendations from LLM
            recommended_coupons = self.llm_client.get_coupon_recommendations(prompt)
            
            # Store results
            recommendations_dict[user_id] = recommended_coupons
            
            # Create recommendation object for database
            recommendation = Recommendation(
                user_id=user_id,
                coupon_ids=recommended_coupons,
                created_at=datetime.utcnow(),
                recommendation_id=str(uuid.uuid4())
            )
            recommendations_to_store.append(recommendation)
        
        # Step 6: Store recommendations in Aurora
        self.db_manager.store_recommendations(recommendations_to_store)
        
        return recommendations_dict

def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    AWS Lambda handler function
    
    Args:
        event (Dict[str, Any]): Lambda event
        context (Any): Lambda context
        
    Returns:
        Dict[str, Any]: Response with user recommendations
    """
    try:
        # Initialize the recommender
        recommender = FinanceRecommender()
        
        # Get the file key from the event or use default
        file_key = event.get('file_key', f"{TRANSACTIONS_PREFIX}latest.csv")
        
        # Process recommendations
        recommendations = recommender.process_user_recommendations(file_key)
        
        # Return results for the first user (assuming single user processing)
        if recommendations:
            user_id = list(recommendations.keys())[0]
            return {
                "statusCode": 200,
                "body": {
                    "userid": user_id,
                    "recommendations": recommendations[user_id]
                }
            }
        else:
            return {
                "statusCode": 404,
                "body": "No recommendations found"
            }
            
    except Exception as e:
        return {
            "statusCode": 500,
            "body": f"Error processing recommendations: {str(e)}"
        }
