import psycopg2
from typing import List, Dict
from datetime import datetime
import uuid
from contextlib import contextmanager

from models import User, Coupon, Recommendation
from config import (
    DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD,
    USERS_TABLE, COUPONS_TABLE, RECOMMENDATIONS_TABLE
)

class DatabaseManager:
    def __init__(self):
        self.connection_params = {
            'host': DB_HOST,
            'port': DB_PORT,
            'database': DB_NAME,
            'user': DB_USER,
            'password': DB_PASSWORD
        }

    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = psycopg2.connect(**self.connection_params)
        try:
            yield conn
        finally:
            conn.close()

    def get_users_info(self, user_ids: List[int]) -> Dict[int, User]:
        """
        Fetch user information for given user IDs
        
        Args:
            user_ids (List[int]): List of user IDs
            
        Returns:
            Dict[int, User]: Dictionary mapping user IDs to User objects
        """
        query = f"""
            SELECT user_id, name, age, income_bracket, 
                   preferred_categories, email
            FROM {USERS_TABLE}
            WHERE user_id = ANY(%s)
        """
        
        users = {}
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (user_ids,))
                for row in cur.fetchall():
                    user = User(
                        user_id=row[0],
                        name=row[1],
                        age=row[2],
                        income_bracket=row[3],
                        preferred_categories=row[4],
                        email=row[5]
                    )
                    users[user.user_id] = user
        
        return users

    def get_all_coupons(self) -> List[Coupon]:
        """
        Fetch all available coupons
        
        Returns:
            List[Coupon]: List of all coupons
        """
        query = f"""
            SELECT coupon_id, description, discount_amount, 
                   category, merchant, expiry_date, min_purchase_amount
            FROM {COUPONS_TABLE}
            WHERE expiry_date > NOW()
        """
        
        coupons = []
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                for row in cur.fetchall():
                    coupon = Coupon(
                        coupon_id=row[0],
                        description=row[1],
                        discount_amount=row[2],
                        category=row[3],
                        merchant=row[4],
                        expiry_date=row[5],
                        min_purchase_amount=row[6]
                    )
                    coupons.append(coupon)
        
        return coupons

    def store_recommendations(self, recommendations: List[Recommendation]) -> None:
        """
        Store recommendations in the database
        
        Args:
            recommendations (List[Recommendation]): List of recommendations to store
        """
        query = f"""
            INSERT INTO {RECOMMENDATIONS_TABLE}
            (recommendation_id, user_id, coupon_ids, created_at)
            VALUES (%s, %s, %s, %s)
        """
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                for rec in recommendations:
                    cur.execute(query, (
                        rec.recommendation_id,
                        rec.user_id,
                        rec.coupon_ids,
                        rec.created_at
                    ))
            conn.commit()
