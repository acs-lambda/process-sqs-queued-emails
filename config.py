# config.py
import os

# Load configuration from environment variables
BUCKET_NAME = os.environ['BUCKET_NAME']
QUEUE_URL = os.environ['QUEUE_URL']
PROCESSING_LAMBDA_ARN = os.environ['PROCESSING_LAMBDA_ARN']
GENERATE_EV_LAMBDA_ARN = os.environ['GENERATE_EV_LAMBDA_ARN']
LCP_LLM_RESPONSE_LAMBDA_ARN = os.environ['LCP_LLM_RESPONSE_LAMBDA_ARN']
DB_SELECT_LAMBDA = os.environ['DB_SELECT_LAMBDA']  # Database Lambda function name
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-2')

# Spam configuration
SPAM_TTL_DAYS = int(os.environ.get('SPAM_TTL_DAYS', 30))  # Default 30 days TTL for spam emails
