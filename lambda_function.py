# handler.py
import json
import uuid
import base64
from datetime import datetime, timedelta
import boto3
import logging
from typing import Dict, Any, Optional

from config import BUCKET_NAME, QUEUE_URL, AWS_REGION
from parser import parse_email, extract_email_headers, extract_email_from_text
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
        
        # Use both In-Reply-To and References for better threading
        conv_id = get_conversation_id(in_reply_to) or get_conversation_id(references) or str(uuid.uuid4())
        account_id = get_associated_account(destination)
        
        if not account_id:
            logger.error(f"No account found for destination: {destination}")
            return None

        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
        is_first = not bool(in_reply_to or references)

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
            'text_body': text_body
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

        if data['is_first']:
            # Store in Threads table for new conversations
            threads_table = dynamodb.Table('Threads')
            threads_table.put_item(
                Item={
                    'conversation_id': data['conv_id'],
                    'source': data['source'],
                    'associated_account': data['account_id'],
                    'read': False,
                    'lcp_enabled': True
                }
            )

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

                # Store the email data with the EV score
                if not store_email_data(email_data, ev):
                    continue

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
