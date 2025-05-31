# handler.py
import json
import uuid
import base64
from datetime import datetime, timedelta
import boto3
import logging
from typing import Dict, Any, Optional

from config import BUCKET_NAME, QUEUE_URL, AWS_REGION, GENERATE_EV_LAMBDA_ARN, LCP_LLM_RESPONSE_LAMBDA_ARN
from parser import parse_email, extract_email_headers, extract_email_from_text, extract_user_info_from_headers
from db import get_conversation_id, get_associated_account, get_email_chain, get_account_email, update_thread_attributes, store_conversation_item, update_thread_read_status, store_thread_item
from scheduling import generate_safe_schedule_name, schedule_email_processing

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
sqs = boto3.client('sqs')
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)
lambda_client = boto3.client('lambda', region_name=AWS_REGION)

def update_thread_with_attributes(conversation_id: str) -> None:
    """
    Invokes get-thread-attrs lambda and updates the thread with the returned attributes.
    """
    try:
        # Invoke get-thread-attrs lambda
        response = lambda_client.invoke(
            FunctionName='getThreadAttrs',
            InvocationType='RequestResponse',
            Payload=json.dumps({
                'body': json.dumps({
                    'conversationId': conversation_id
                })
            })
        )
        
        # Parse the response
        response_payload = json.loads(response['Payload'].read())
        if response_payload['statusCode'] != 200:
            logger.error(f"Failed to get thread attributes: {response_payload}")
            return
            
        attributes = json.loads(response_payload['body'])
        
        # Convert attribute names to lowercase with underscores
        formatted_attributes = {
            key.lower().replace(' ', '_'): value 
            for key, value in attributes.items()
        }
        
        # Update the thread using db-select
        if not update_thread_attributes(conversation_id, formatted_attributes):
            logger.error(f"Failed to update thread attributes for conversation {conversation_id}")
            return
            
        logger.info(f"Successfully updated thread attributes for conversation {conversation_id}")
    except Exception as e:
        logger.error(f"Error updating thread attributes: {str(e)}")

def process_email_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Process a single SQS record containing an email.
    Returns the processed data or None if processing failed.
    """
    try:
        body = json.loads(record['body'])
        mail = json.loads(body['Message'])['mail']

        source = mail['source']
        destination = mail['destination'][0]
        subject = mail['commonHeaders'].get('subject', '')
        s3_key = mail['messageId']

        # Fetch and parse email
        raw = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)['Body'].read()
        msg, text_body = parse_email(raw)
        
        if not msg or not text_body:
            logger.error("Failed to parse email content")
            return None

        msg_id_hdr, in_reply_to, references = extract_email_headers(msg)
        user_info = extract_user_info_from_headers(msg)
        
        # Use both In-Reply-To and References for better threading
        conv_id = None
        if in_reply_to:
            conv_id = get_conversation_id(in_reply_to)
            logger.info(f"Found conversation ID from in_reply_to: {conv_id}")
        if not conv_id and references:
            conv_id = get_conversation_id(references)
            logger.info(f"Found conversation ID from references: {conv_id}")
        
        # Only generate new UUID if we couldn't find an existing conversation
        if not conv_id:
            conv_id = str(uuid.uuid4())
            logger.info(f"Generated new conversation ID: {conv_id}")
        
        account_id = get_associated_account(destination)
        
        if not account_id:
            logger.error(f"No account found for destination: {destination}")
            return None

        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        is_first = not bool(in_reply_to or references)
        logger.info(f"Email is_first: {is_first}, conv_id: {conv_id}, in_reply_to: {in_reply_to}, references: {references}")

        return {
            'source': source,
            'destination': destination,
            'subject': subject,
            's3_key': s3_key,
            'msg_id_hdr': msg_id_hdr,
            'in_reply_to': in_reply_to,
            'references': references,
            'conv_id': conv_id,
            'account_id': account_id,
            'timestamp': timestamp,
            'is_first': is_first,
            'text_body': text_body,
            'user_info': user_info
        }
    except Exception as e:
        logger.error(f"Error processing email record: {str(e)}")
        return None

def store_email_data(data: Dict[str, Any]) -> bool:
    """
    Store email data in DynamoDB tables.
    Uses db-select for reads and direct DynamoDB access for writes.
    Returns True if successful, False otherwise.
    """
    try:
        # Get sender name from user_info if available
        sender_name = data['user_info'].get('sender_name', '')
        
        # Prepare conversation data
        conversation_data = {
            'conversation_id': data['conv_id'],
            'response_id': data['msg_id_hdr'],
            'in_reply_to': data['in_reply_to'],
            'timestamp': data['timestamp'],
            'sender': data['source'],
            'receiver': data['destination'],
            'associated_account': data['account_id'],
            'subject': data['subject'],
            'body': data['text_body'],
            's3_location': data['s3_key'],
            'type': 'inbound-email',
            'is_first_email': '1' if data['is_first'] else '0'
        }
        
        # Store in Conversations table using direct DynamoDB access
        if not store_conversation_item(conversation_data):
            logger.error(f"Failed to store conversation data for {data['conv_id']}")
            return False

        # Prepare thread data
        thread_data = {
            'conversation_id': data['conv_id'],
            'source': data['source'],
            'source_name': sender_name,
            'associated_account': data['account_id'],
            'read': False,
            'lcp_enabled': True,
            'lcp_flag_threshold': '80',
            'flag': False  # Will be updated by generate-ev lambda
        }
        
        # Check if thread exists using db-select
        existing_thread = invoke_db_select(
            table_name='Threads',
            index_name=None,  # Primary key query
            key_name='conversation_id',
            key_value=data['conv_id']
        )
        
        if data['is_first'] and not existing_thread:
            # Only create new thread if it's first email and thread doesn't exist
            logger.info(f"Creating new thread for conversation {data['conv_id']}")
            if not store_thread_item(thread_data):
                logger.error(f"Failed to create thread for {data['conv_id']}")
                return False
                
        elif existing_thread:
            # Update existing thread using direct DynamoDB access
            logger.info(f"Updating existing thread for conversation {data['conv_id']}")
            if not update_thread_read_status(data['conv_id'], False):
                logger.error(f"Failed to update thread for {data['conv_id']}")
                return False
        else:
            logger.warning(f"Thread not found for non-first email conversation {data['conv_id']}")

        # Update thread attributes after storing email data
        update_thread_with_attributes(data['conv_id'])

        return True
    except Exception as e:
        logger.error(f"Error storing email data: {str(e)}")
        return False

def invoke_generate_ev(conversation_id: str, message_id: str, account_id: str) -> Optional[int]:
    """
    Invokes the generate-ev lambda to calculate and update EV score.
    Returns the EV score if successful, None otherwise.
    """
    try:
        response = lambda_client.invoke(
            FunctionName=GENERATE_EV_LAMBDA_ARN,
            InvocationType='RequestResponse',
            Payload=json.dumps({
                'conversation_id': conversation_id,
                'message_id': message_id,
                'account_id': account_id
            })
        )
        
        response_payload = json.loads(response['Payload'].read())
        if response_payload['statusCode'] != 200:
            logger.error(f"Failed to generate EV: {response_payload}")
            return None
            
        result = json.loads(response_payload['body'])
        if result['status'] != 'success':
            logger.error(f"Generate EV failed: {result}")
            return None
            
        return result['ev_score']
    except Exception as e:
        logger.error(f"Error invoking generate-ev lambda: {str(e)}")
        return None

def invoke_llm_response(conversation_id: str, account_id: str, is_first_email: bool) -> Optional[str]:
    """
    Invokes the lcp-llm-response lambda to generate a response.
    Returns the generated response if successful, None otherwise.
    """
    try:
        response = lambda_client.invoke(
            FunctionName=LCP_LLM_RESPONSE_LAMBDA_ARN,
            InvocationType='RequestResponse',
            Payload=json.dumps({
                'conversation_id': conversation_id,
                'account_id': account_id,
                'is_first_email': is_first_email
            })
        )
        
        response_payload = json.loads(response['Payload'].read())
        if response_payload['statusCode'] != 200:
            logger.error(f"Failed to generate LLM response: {response_payload}")
            return None
            
        result = json.loads(response_payload['body'])
        if result['status'] != 'success':
            logger.error(f"LLM response generation failed: {result}")
            return None
            
        return result['response']
    except Exception as e:
        logger.error(f"Error invoking lcp-llm-response lambda: {str(e)}")
        return None

def lambda_handler(event, context):
    """
    Main lambda handler for processing SQS queued emails.
    """
    try:
        all_records = event.get('Records', [])
        if not all_records:
            logger.warning("No records found in event")
            return {'statusCode': 200, 'body': 'No records to process'}

        for record in all_records:
            try:
                # Process the email
                email_data = process_email_record(record)
                if not email_data:
                    continue

                # Store the email data
                if not store_email_data(email_data):
                    continue

                # Calculate EV using the generate-ev lambda
                ev = invoke_generate_ev(
                    email_data['conv_id'],
                    email_data['msg_id_hdr'],
                    email_data['account_id']
                )
                
                if ev is None:
                    logger.error(f"Failed to calculate EV for conversation {email_data['conv_id']}")
                    continue

                logger.info(f"EV score calculated: {ev} for conversation {email_data['conv_id']}")

                # Get thread information to check thresholds
                threads_table = dynamodb.Table('Threads')
                thread_response = threads_table.get_item(
                    Key={
                        'conversation_id': email_data['conv_id']
                    }
                )
                
                # Get threshold from thread or use default
                threshold = 80
                if 'Item' in thread_response:
                    # Convert Decimal to int if it exists
                    threshold = int(thread_response['Item'].get('lcp_flag_threshold', 80))
                
                # Check if we should generate and send response
                should_generate_response = True
                if not email_data['is_first']:
                    if 'Item' in thread_response:
                        lcp_enabled = thread_response['Item'].get('lcp_enabled', 'false')
                        should_generate_response = lcp_enabled == True
                        logger.info(f"Thread lcp_enabled value: {lcp_enabled}, will generate response: {should_generate_response}")

                if should_generate_response:
                    # Generate response using the lcp-llm-response lambda
                    response = invoke_llm_response(
                        email_data['conv_id'],
                        email_data['account_id'],
                        email_data['is_first']
                    )
                    
                    if response is None:
                        logger.error(f"Failed to generate response for conversation {email_data['conv_id']}")
                        continue

                    # Prepare and schedule the response
                    payload = {
                        'response_body': response,
                        'account': email_data['account_id'],
                        'target': email_data['source'],
                        'in_reply_to': email_data['msg_id_hdr'],
                        'conversation_id': email_data['conv_id'],
                        'subject': email_data['subject'],
                        'ev_score': ev
                    }

                    schedule_name = generate_safe_schedule_name(
                        f"process-email-{''.join(c for c in email_data['msg_id_hdr'] if c.isalnum())}"
                    )
                    schedule_time = datetime.utcnow() + timedelta(seconds=10)
                    
                    schedule_email_processing(
                        schedule_name,
                        schedule_time,
                        payload,
                        email_data['in_reply_to']
                    )
                else:
                    logger.info(f"Skipping response generation for conversation {email_data['conv_id']} as lcp_enabled is not 'true'")

                # Only delete from SQS after successful processing
                sqs.delete_message(
                    QueueUrl=QUEUE_URL,
                    ReceiptHandle=record['receiptHandle']
                )

            except Exception as e:
                logger.error(f"Error processing record: {str(e)}")
                continue

        return {'statusCode': 200, 'body': 'Success'}
    except Exception as e:
        logger.error(f"Error in lambda handler: {str(e)}")
        return {'statusCode': 500, 'body': str(e)}
