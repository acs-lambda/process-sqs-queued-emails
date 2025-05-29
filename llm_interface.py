import json
import requests
import boto3

tai_key = "2e1a1e910693ae18c09ad0585a7645e0f4595e90ec35bb366b6f5520221b6ca7"
url = "https://api.together.xyz/v1/chat/completions"

dynamodb = boto3.client('dynamodb')
dynamodb_resource = boto3.resource('dynamodb')

realtor_role = {"role": "system", "content": "You are a real estate agent. Respond as if you are trying to follow up to this potential client. This email was auto-generated and a personalized response to follow up with the user should be sent based on the contents given to you. Remember to respond as the agent and not the client. You are answering whatever questions the client asks."}

def send_message_to_llm(messages):
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
    print(payload)
    response = requests.post(url, headers=headers, json=payload)
    response_data = response.json()

    # Ensure proper error handling in case the API call fails
    if response.status_code != 200 or "choices" not in response_data:
        raise Exception("Failed to fetch response from Together AI API", response_data)

    return response_data["choices"][0]["message"]["content"]

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
    :param email_chain: List of emails in the chain.
    :return: List of formatted messages for the LLM.
    """
    formatted_messages = [realtor_role]
    
    for email in email_chain:
        formatted_messages.append({"role": "email", "content": email['body']})
    
    return formatted_messages


def send_introductory_email(starting_msg, uid):
    # template = get_template_to_use(uid)
    return send_message_to_llm([realtor_role, {"role": "system", "content": "Make sure to not include anything else in your response other than the response you give as you emulate a realtor."}, {"role": "user", "content": "Subject: " +starting_msg['subject'] +"\n\nBody:" + starting_msg['body']}])

def generate_email_response(emails, uid):
    if len(emails) == 1:
        return send_introductory_email(emails[0], uid)
    formatted_conversation = format_conversation_for_llm(emails)
    
    response = send_message_to_llm(formatted_conversation)
    print("Response from llm interface: " + response)
    return response
