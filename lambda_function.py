# handler.py
import json
import uuid
import base64
from datetime import datetime, timedelta
import boto3

from config import BUCKET_NAME, QUEUE_URL, AWS_REGION
from parser import parse_email, extract_email_headers, extract_email_from_text
from db import get_conversation_id, get_associated_account, get_email_chain, get_account_email
from scheduling import generate_safe_schedule_name, schedule_email_processing
from ev_calculator import calc_ev, parse_messages
from llm_interface import generate_email_response

s3 = boto3.client('s3')
sqs = boto3.client('sqs')
dynamodb = boto3.resource("dynamodb", region_name=AWS_REGION)

def lambda_handler(event, context):
    record = None
    print("Event: ", event)
    all_records = event.get('Records', [])
    print("Number of records: ", len(all_records))
    if len(all_records) > 1:
        print("More than one record found - WARNING")
    record = all_records[0]

    # No matter what, remove from SQS
    receipt_handle = record['receiptHandle']
    sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle)


    body = json.loads(record['body'])
    mail = json.loads(body['Message'])['mail']

    source = mail['source']
    destination = mail['destination'][0]
    subject = mail['commonHeaders'].get('subject', '')
    s3_key = mail['messageId']

    # Fetch and parse email
    raw = s3.get_object(Bucket=BUCKET_NAME, Key=s3_key)['Body'].read()
    msg, text_body = parse_email(raw)
    msg_id_hdr, in_reply_to, _ = extract_email_headers(msg)

    print("msg_id_hdr: ", msg_id_hdr)
    print("in_reply_to: ", in_reply_to)

    # Determine conversation context
    conv_id = get_conversation_id(in_reply_to) or str(uuid.uuid4())
    print("Conversation ID: ", conv_id)
    account_id = get_associated_account(destination)
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    is_first = not bool(in_reply_to)


    # Determine if recipient exists
    table = dynamodb.Table('Users')
    # if internal domain, partition by id
    if destination.lower().endswith('@lgw.automatedconsultancy.com'):
        key_cond = {'id': destination.split('@')[0]}
        return table.get_item(Key=key_cond).get('Item', {}).get('id')
    # otherwise lookup by responseEmail index
    res = table.query(
        IndexName='responseEmail-index',
        KeyConditionExpression='responseEmail = :e',
        ExpressionAttributeValues={':e': destination.lower()}
    )
    items = res.get('Items', [])
    if not items:
        print("No user with the recipient id " + destination.lower() + " found")
        return

    is_first_B = '1' if is_first else '0'
    print("Account ID: ", account_id)


    # Prepare scheduling
    base_name = f"process-email-{''.join(c for c in msg_id_hdr if c.isalnum())}"
    schedule_name = generate_safe_schedule_name(base_name)
    schedule_time = datetime.utcnow() + timedelta(seconds=10)

    response = None
    ev = -1
    if is_first:
        # store new thread
        # Note: Certain items do not have fallbacks. This is because they are required for items (keys or sort keys)

        chain = get_email_chain(conv_id)
        chain.append({'sender': source, 'body': text_body})
        realtor_email = get_account_email(account_id)
        ev = calc_ev(parse_messages(realtor_email, chain))


        table = dynamodb.Table('Threads')
        table.put_item(Item={
            'conversation_id': conv_id,
            'source': source or '',
            'associated_account': account_id,
            'read': False,
            'lcp_enabled': True
        })

        # Store new instance in Conversations

        table = dynamodb.Table('Conversations')
        sender_addr = source
        table.put_item(Item={
            'conversation_id': conv_id,
            'response_id': msg_id_hdr,
            'timestamp': timestamp or '',
            'sender': source or '',
            'receiver': destination or '',
            'associated_account': account_id,
            'subject': subject or '',
            'body': text_body or '',
            's3_location': s3_key or '',
            'type': 'inbound-email' or '',
            'is_first_email': is_first_B or '0',
            "ev_score": str(ev) or '-1'
        })


        # Initial LLM response
        response = generate_email_response([{'subject': subject, 'body': text_body}], account_id)
        payload = {
            'response_body': response,
            'account': account_id,
            'target': sender_addr,
            'in_reply_to': msg_id_hdr,
            'conversation_id': conv_id,
            'subject': subject,
        }
    else:
        # Retrieve the conversation from "Threads" table
        table = dynamodb.Table('Threads')

        # Retrieve the conversation from "Threads" table
        response = table.get_item(Key={'conversation_id': conv_id})

        print("Existing_table response: ", response)

        # Calculate EV and then generate response on full chain
        chain = get_email_chain(conv_id)
        chain.append({'sender': source, 'body': text_body})
        realtor_email = get_account_email(account_id)
        ev = calc_ev(parse_messages(realtor_email, chain))
        # Optionally store EV score here
        print(chain)
        print(account_id)
        response = generate_email_response(chain, account_id)
        payload = {
            'response_body': response,
            'account': account_id,
            'target': source,
            'in_reply_to': msg_id_hdr,
            'conversation_id': conv_id,
            'ev_score': ev
        }

    # Schedule the processing
    schedule_email_processing(schedule_name, schedule_time, payload, in_reply_to)


    return {
        'statusCode': 200,
        'body': json.dumps('Success')
    }
