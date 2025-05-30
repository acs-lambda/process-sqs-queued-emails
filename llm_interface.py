import json
import requests
import boto3
import logging

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

tai_key = "2e1a1e910693ae18c09ad0585a7645e0f4595e90ec35bb366b6f5520221b6ca7"
url = "https://api.together.xyz/v1/chat/completions"

dynamodb = boto3.client('dynamodb')
dynamodb_resource = boto3.resource('dynamodb')

realtor_role = {"role": "system", "content": "You are a real estate agent. Respond as if you are trying to follow up to this potential client. This email was auto-generated and a personalized response to follow up with the user should be sent based on the contents given to you. Remember to respond as the agent and not the client. You are answering whatever questions the client asks."}

def send_message_to_llm(messages):
    """
    Sends messages to the LLM API and returns the response.
    """
    headers = {
        "Authorization": f"Bearer {tai_key}",
        "Content-Type": "application/json"
    }

    payload = {
        "model": "meta-llama/Llama-4-Maverick-17B-128E-Instruct-FP8",
        "messages": messages,
        "max_tokens": 512,
        "temperature": 0.7,
        "top_p": 0.7,
        "top_k": 50,
        "repetition_penalty": 1,
        "stop": ["<|im_end|>", "<|endoftext|>"],
        "stream": False
    }
    
    try:
        logger.info("Sending request to Together AI API")
        response = requests.post(url, headers=headers, json=payload)
        response_data = response.json()

        if response.status_code != 200 or "choices" not in response_data:
            logger.error(f"API call failed: {response_data}")
            raise Exception("Failed to fetch response from Together AI API", response_data)

        return response_data["choices"][0]["message"]["content"]
    except Exception as e:
        logger.error(f"Error in send_message_to_llm: {str(e)}")
        raise

def get_template_to_use(uid, email_type):
    table = dynamodb_resource.Table('Templates')

    response = table.query(
        KeyConditionExpression=Key('uid').eq('user123') & Key('activated').eq(b'\x01')
    )

    if response.get('Items'):
        for response in response['Items']:
            if response['email_type'] == email_type:
                return response['content']
        return ""

def format_conversation_for_llm(email_chain):
    """
    Formats the email chain to be compatible with the LLM input structure.
    Includes both subject and body for each email.
    """
    formatted_messages = [realtor_role]
    
    logger.info(f"Formatting conversation for LLM. Chain length: {len(email_chain)}")
    for i, email in enumerate(email_chain):
        # Format each email with both subject and body
        email_content = f"Subject: {email.get('subject', '')}\n\nBody: {email.get('body', '')}"
        role = "user" if email.get('type') == 'inbound-email' else "assistant"
        logger.info(f"Email {i+1} - Role: {role}, Subject: {email.get('subject', '')}, Body length: {len(email.get('body', ''))}")
        
        formatted_messages.append({
            "role": role,
            "content": email_content
        })
    
    return formatted_messages


def send_introductory_email(starting_msg, uid):
    # template = get_template_to_use(uid)
    return send_message_to_llm([
        realtor_role,
        {"role": "system", "content": "ONLY output the body of the email reply. Do NOT include the subject, signature, closing, sender name, or any extra text. Only the main message body as you would write it in the email editor."},
        {"role": "user", "content": "Subject: " +starting_msg['subject'] +"\n\nBody:" + starting_msg['body']}
    ])

def generate_email_response(emails, uid):
    """
    Generates an email response based on the email chain.
    Handles both first-time and subsequent emails consistently.
    """
    try:
        if not emails:
            logger.error("Empty email chain provided")
            raise ValueError("Empty email chain")

        logger.info(f"Generating email response for chain of {len(emails)} emails")
        for i, email in enumerate(emails):
            logger.info(f"Input email {i+1}:")
            logger.info(f"  Subject: {email.get('subject', '')}")
            logger.info(f"  Body: {email.get('body', '')[:100]}...")  # First 100 chars
            logger.info(f"  Type: {email.get('type', 'unknown')}")

        # Format the conversation for the LLM
        formatted_messages = format_conversation_for_llm(emails)
        
        # Add the system message for consistent formatting
        formatted_messages.append({
            "role": "system",
            "content": "ONLY output the body of the email reply. Do NOT include the subject, signature, closing, sender name, or any extra text. Only the main message body as you would write it in the email editor."
        })
        
        # Get response from LLM
        response = send_message_to_llm(formatted_messages)
        logger.info(f"Generated response length: {len(response)}")
        logger.info(f"Response preview: {response[:100]}...")  # First 100 chars
        return response
        
    except Exception as e:
        logger.error(f"Error generating email response: {str(e)}")
        raise
