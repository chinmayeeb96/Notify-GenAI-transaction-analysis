from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime

@dataclass
class Transaction:
    user_id: int
    amount: float
    category: str
    merchant: str
    transaction_date: datetime
    transaction_id: str

@dataclass
class User:
    user_id: int
    name: str
    age: int
    income_bracket: str
    preferred_categories: List[str]
    email: str

@dataclass
class Coupon:
    coupon_id: str
    description: str
    discount_amount: float
    category: str
    merchant: str
    expiry_date: datetime
    min_purchase_amount: Optional[float] = None

@dataclass
class Recommendation:
    user_id: int
    coupon_ids: List[str]
    created_at: datetime
    recommendation_id: str
