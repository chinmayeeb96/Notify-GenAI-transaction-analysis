import boto3
import pandas as pd
from typing import List
from io import StringIO
from models import Transaction
from config import S3_BUCKET_NAME

class S3Manager:
    def __init__(self):
        self.s3_client = boto3.client('s3')

    def read_transactions_csv(self, file_key: str) -> List[Transaction]:
        """
        Read transactions data from S3 CSV file
        
        Args:
            file_key (str): S3 key for the CSV file
            
        Returns:
            List[Transaction]: List of Transaction objects
        """
        try:
            response = self.s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=file_key)
            csv_content = response['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(csv_content))
            
            transactions = []
            for _, row in df.iterrows():
                transaction = Transaction(
                    user_id=row['user_id'],
                    amount=row['amount'],
                    category=row['category'],
                    merchant=row['merchant'],
                    transaction_date=pd.to_datetime(row['transaction_date']),
                    transaction_id=row['transaction_id']
                )
                transactions.append(transaction)
                
            return transactions
        
        except Exception as e:
            raise Exception(f"Error reading transactions from S3: {str(e)}")

    def get_unique_user_ids(self, transactions: List[Transaction]) -> List[int]:
        """
        Extract unique user IDs from transactions
        
        Args:
            transactions (List[Transaction]): List of transactions
            
        Returns:
            List[int]: List of unique user IDs
        """
        return list(set(t.user_id for t in transactions))
