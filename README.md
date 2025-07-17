# Notify-GenAI-transaction-analysis


AI-powered transaction analysis system using AWS Bedrock and LangChain.

## Features
- Transaction pattern analysis
- User spending behavior insights
- Personalized financial product recommendations
- JSON-based insights generation
- Notify the user about the insights

## Requirements
See `requirements.txt` for dependencies.

## Setup
1. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

2. Configure AWS credentials using one of these methods:
   - Set environment variables:
     ```bash
     export AWS_ACCESS_KEY_ID=your_access_key_id
     export AWS_SECRET_ACCESS_KEY=your_secret_access_key
     export AWS_SESSION_TOKEN=your_session_token  # if using temporary credentials
     export AWS_REGION=us-east-1
     ```
   - Use AWS CLI: `aws configure`
   - Use IAM role (if running on AWS)

3. Run the Jupyter notebook

## Data
- Transaction data analysis
- Fixed deposit product recommendations