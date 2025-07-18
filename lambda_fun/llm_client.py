import boto3
from langchain_community.llms.bedrock import Bedrock
from typing import List
import re

from config import BEDROCK_MODEL_ID, AWS_REGION

class LLMClient:
    def __init__(self):
        self.bedrock_runtime = boto3.client(
            service_name="bedrock-runtime",
            region_name=AWS_REGION
        )
        
        self.llm = Bedrock(
            model_id=BEDROCK_MODEL_ID,
            client=self.bedrock_runtime,
            model_kwargs={
                "temperature": 0.7,
                "top_p": 0.7,
                "max_tokens": 1024
            }
        )

    def get_coupon_recommendations(self, prompt: str) -> List[str]:
        """
        Get coupon recommendations from the LLM
        
        Args:
            prompt (str): Generated prompt for coupon recommendations
            
        Returns:
            List[str]: List of recommended coupon IDs
        """
        try:
            response = self.llm.predict(prompt)
            
            # Extract coupon IDs from the response using regex
            coupon_ids = []
            pattern = r'\[([\w\-]+)\]'
            matches = re.findall(pattern, response)
            
            # Take the first 3 matches or all if less than 3
            return matches[:3]
            
        except Exception as e:
            raise Exception(f"Error getting recommendations from LLM: {str(e)}")
