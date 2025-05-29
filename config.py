# config.py
import os

# Load configuration from environment variables
BUCKET_NAME = os.environ['BUCKET_NAME']
QUEUE_URL = os.environ['QUEUE_URL']
PROCESSING_LAMBDA_ARN = os.environ['PROCESSING_LAMBDA_ARN']
AWS_REGION = os.environ.get('AWS_REGION', 'us-east-2')
