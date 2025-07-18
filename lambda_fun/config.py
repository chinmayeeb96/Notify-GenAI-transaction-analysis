import os

# S3 Configuration
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
TRANSACTIONS_PREFIX = "transactions/"

# Database Configuration
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# LLM Configuration
BEDROCK_MODEL_ID = "anthropic.claude-3-5-sonnet-20240620-v1:0"
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# Table Names
USERS_TABLE = "users"
COUPONS_TABLE = "coupons"
RECOMMENDATIONS_TABLE = "recommendations"
