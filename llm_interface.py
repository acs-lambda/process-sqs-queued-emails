import json
import requests
import boto3
import logging
from db import store_ai_invocation

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

tai_key = "2e1a1e910693ae18c09ad0585a7645e0f4595e90ec35bb366b6f5520221b6ca7"
url = "https://api.together.xyz/v1/chat/completions"

spam_detection_role = {
    "role": "system",
    "content": """You are a spam detection system for a real estate automation platform. Your job is to determine if an email is relevant to real estate conversations or if it should be classified as spam.

CLASSIFY AS SPAM if the email is:
- Marketing/promotional emails unrelated to real estate
- Newsletter subscriptions
- Social media notifications
- Online shopping confirmations/receipts
- Technical notifications (server alerts, software updates, etc.)
- Personal emails clearly unrelated to real estate business
- Automated system emails from non-real estate platforms
- Job postings unrelated to real estate

CLASSIFY AS NOT SPAM if the email is:
- Inquiries about buying/selling/renting property
- Questions about real estate services
- Responses to property listings
- Real estate market inquiries
- Mortgage/financing related to property purchases
- Property management questions
- Real estate investment inquiries
- Follow-up emails about property viewings or consultations

Respond with ONLY the word "spam" or "not spam" - nothing else."""
}

def detect_spam(subject: str, body: str, sender: str, account_id: str) -> bool:
    """
    Uses LLM to detect if an email is spam (not related to real estate conversations).
    Returns True if the email is spam, False otherwise.
    """
    try:
        # Prepare the email content for spam detection
        email_content = f"""
Subject: {subject}
From: {sender}
Body: {body}
"""
        
        messages = [
            spam_detection_role,
            {
                "role": "user",
                "content": email_content
            }
        ]
        
        # Use the LLM API to detect spam
        headers = {
            "Authorization": f"Bearer {tai_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "model": "meta-llama/Llama-4-Maverick-17B-128E-Instruct-FP8",
            "messages": messages,
            "max_tokens": 10,  # We only need "spam" or "not spam"
            "temperature": 0.1,  # Low temperature for consistent classification
            "top_p": 0.9,
            "top_k": 50,
            "repetition_penalty": 1,
            "stop": ["<|im_end|>", "<|endoftext|>"],
            "stream": False
        }
        
        logger.info(f"Checking spam for email from {sender} with subject: {subject}")
        response = requests.post(url, headers=headers, json=payload)
        response_data = response.json()

        if response.status_code != 200 or "choices" not in response_data:
            logger.error(f"Spam detection API call failed: {response_data}")
            # In case of API failure, assume not spam to avoid false positives
            return False

        logger.info("Raw response data: %s", response_data)

        response_text = response_data["choices"][0]["message"]["content"].strip().lower()
        logger.info(f"Spam detection response: {response_text}")
        
        # Get token usage from the API response
        usage = response_data.get("usage", {})
        input_tokens = usage.get("prompt_tokens", 0)
        output_tokens = usage.get("completion_tokens", 0)
        
        # Store the invocation record with actual token counts
        store_ai_invocation(
            associated_account=account_id,
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            llm_email_type="spam_detection"
        )
        
        # Check if the response contains "spam"
        is_spam = "spam" in response_text and "not spam" not in response_text
        logger.info(f"Email classified as spam: {is_spam}")
        
        return is_spam
        
    except Exception as e:
        logger.error(f"Error in spam detection: {str(e)}")
        # In case of error, assume not spam to avoid false positives
        return False
