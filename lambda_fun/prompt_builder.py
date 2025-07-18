from typing import List
from models import User, Transaction, Coupon

class PromptBuilder:
    @staticmethod
    def create_recommendation_prompt(
        user: User,
        transactions: List[Transaction],
        coupons: List[Coupon]
    ) -> str:
        """
        Create a prompt for the LLM to recommend coupons based on user data
        
        Args:
            user (User): User information
            transactions (List[Transaction]): User's transaction history
            coupons (List[Coupon]): Available coupons
            
        Returns:
            str: Generated prompt for the LLM
        """
        # Create user profile section
        user_profile = f"""
        User Profile:
        - Name: {user.name}
        - Age: {user.age}
        - Income Bracket: {user.income_bracket}
        - Preferred Categories: {', '.join(user.preferred_categories)}
        """

        # Create transaction history section
        transaction_history = "Recent Transactions:\n"
        for t in sorted(transactions, key=lambda x: x.transaction_date, reverse=True)[:10]:
            transaction_history += f"- {t.transaction_date.strftime('%Y-%m-%d')}: {t.merchant} - {t.category} - ${t.amount:.2f}\n"

        # Create available coupons section
        coupons_info = "Available Coupons:\n"
        for c in coupons:
            coupons_info += f"- {c.coupon_id}: {c.description} ({c.category} - {c.merchant})\n"

        # Create the final prompt
        prompt = f"""Based on the following information, recommend the top 3 most relevant coupons for this user.
        Consider the user's profile, recent transaction history, and available coupons.
        Prioritize coupons that match the user's spending patterns and preferred categories.

        {user_profile}

        {transaction_history}

        {coupons_info}

        Please provide your recommendations in the following format:
        1. [coupon_id] - Brief explanation of why this coupon is relevant
        2. [coupon_id] - Brief explanation of why this coupon is relevant
        3. [coupon_id] - Brief explanation of why this coupon is relevant"""

        return prompt
