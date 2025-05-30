# handler.py
import json
import uuid
import base64
from datetime import datetime, timedelta
import boto3
import logging
from typing import Dict, Any, Optional

from config import BUCKET_NAME, QUEUE_URL, AWS_REGION
from parser import parse_email, extract_email_headers, extract_email_from_text, extract_user_info_from_headers
from db import get_conversation_id, get_associated_account, get_email_chain, get_account_email
from scheduling import generate_safe_schedule_name, schedule_email_processing
from ev_calculator import calc_ev, parse_messages
from llm_interface import generate_email_response

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.client('s3')
sqs = boto3.client('sqs')
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)

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

def store_email_data(data: Dict[str, Any], ev_score: int) -> bool:
    """
    Store email data in DynamoDB tables.
    Returns True if successful, False otherwise.
    """
    try:
        # Get sender name from user_info if available
        sender_name = data['user_info'].get('sender_name', '')
        
        # Store in Conversations table
        conversations_table = dynamodb.Table('Conversations')
        conversations_table.put_item(
            Item={
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
                'is_first_email': '1' if data['is_first'] else '0',
                'ev_score': str(ev_score)
            }
        )

        threads_table = dynamodb.Table('Threads')
        
        # Check if thread exists
        thread_response = threads_table.get_item(
            Key={
                'conversation_id': data['conv_id']
            }
        )
        
        if data['is_first'] and 'Item' not in thread_response:
            # Only create new thread if it's first email and thread doesn't exist
            logger.info(f"Creating new thread for conversation {data['conv_id']}")
            threads_table.put_item(
                Item={
                    'conversation_id': data['conv_id'],
                    'source': data['source'],
                    'source_name': sender_name,
                    'associated_account': data['account_id'],
                    'read': False,
                    'lcp_enabled': True,
                    'lcp_flag_threshold': '80',
                    'flag': ev_score >= 80
                }
            )
        elif 'Item' in thread_response:
            # Update existing thread
            logger.info(f"Updating existing thread for conversation {data['conv_id']}")
            threads_table.update_item(
                Key={
                    'conversation_id': data['conv_id']
                },
                UpdateExpression='SET #read = :read, #flag = :flag',
                ExpressionAttributeNames={
                    '#read': 'read',
                    '#flag': 'flag'
                },
                ExpressionAttributeValues={
                    ':read': False,
                    ':flag': ev_score >= 80
                }
            )
        else:
            logger.warning(f"Thread not found for non-first email conversation {data['conv_id']}")

        return True
    except Exception as e:
        logger.error(f"Error storing email data: {str(e)}")
        return False

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

                # Calculate EV and generate response
                chain = get_email_chain(email_data['conv_id'])
                realtor_email = get_account_email(email_data['account_id'])
                # If the just-processed email is not in the chain, add it for EV calculation
                if not any(item.get('response_id') == email_data['msg_id_hdr'] for item in chain):
                    chain.append({
                        'subject': email_data['subject'],
                        'body': email_data['text_body'],
                        'sender': email_data['source'],
                        'timestamp': email_data['timestamp'],
                        'type': 'inbound-email',
                        'response_id': email_data['msg_id_hdr']
                    })
                ev = calc_ev(parse_messages(realtor_email, chain))
                logger.info(f"EV score calculated: {ev} for conversation {email_data['conv_id']} with chain length {len(chain)}")

                # Get thread information to check thresholds
                threads_table = dynamodb.Table('Threads')
                thread_response = threads_table.get_item(
                    Key={
                        'conversation_id': email_data['conv_id']
                    }
                )
                
                # Get threshold from thread or use default
                threshold = 80  # Default threshold
                if 'Item' in thread_response:
                    # Convert Decimal to int if it exists
                    threshold = int(thread_response['Item'].get('lcp_flag_threshold', 80))
                
                # Store the email data with the EV score
                if not store_email_data(email_data, ev):
                    continue

                # Check if we should generate and send response
                should_generate_response = True
                if not email_data['is_first']:
                    if 'Item' in thread_response:
                        lcp_enabled = thread_response['Item'].get('lcp_enabled', 'false')
                        should_generate_response = lcp_enabled == 'true'
                        logger.info(f"Thread lcp_enabled value: {lcp_enabled}, will generate response: {should_generate_response}")

                if should_generate_response:
                    # Generate response
                    response = generate_email_response(
                        chain if not email_data['is_first'] else [{'subject': email_data['subject'], 'body': email_data['text_body']}],
                        email_data['account_id']
                    )

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
        return {'statusCode': 500, 'body': f'Error: {str(e)}'}
