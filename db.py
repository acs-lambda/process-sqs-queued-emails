# db.py
import json
import boto3
import logging
from typing import Dict, Any, Optional, List
from config import AWS_REGION, DB_SELECT_LAMBDA

logger = logging.getLogger()
logger.setLevel(logging.INFO)

lambda_client = boto3.client('lambda', region_name=AWS_REGION)

def invoke_db_select(table_name: str, index_name: Optional[str], key_name: str, key_value: Any) -> Optional[Dict[str, Any]]:
    """
    Generic function to invoke the db-select Lambda.
    Returns the parsed response or None if the invocation failed.
    """
    try:
        payload = {
            'table_name': table_name,
            'index_name': index_name,
            'key_name': key_name,
            'key_value': key_value
        }
        
        response = lambda_client.invoke(
            FunctionName=DB_SELECT_LAMBDA,
            InvocationType='RequestResponse',
            Payload=json.dumps(payload)
        )
        
        response_payload = json.loads(response['Payload'].read())
        if response_payload['statusCode'] != 200:
            logger.error(f"Database Lambda failed: {response_payload}")
            return None
            
        return json.loads(response_payload['body'])
    except Exception as e:
        logger.error(f"Error invoking database Lambda: {str(e)}")
        return None

def get_conversation_id(message_id: str) -> Optional[str]:
    """Get conversation ID by message ID."""
    if not message_id:
        return None
    
    result = invoke_db_select(
        table_name='Conversations',
        index_name='response_id-index',
        key_name='response_id',
        key_value=message_id
    )
    
    return result.get('conversation_id') if result else None

def get_associated_account(email: str) -> Optional[str]:
    """Get account ID by email."""
    result = invoke_db_select(
        table_name='Users',
        index_name='responseEmail-index',
        key_name='responseEmail',
        key_value=email.lower()
    )
    
    return result.get('id') if result else None

def get_email_chain(conversation_id: str) -> List[Dict[str, Any]]:
    """Get email chain for a conversation."""
    result = invoke_db_select(
        table_name='Conversations',
        index_name=None,  # Primary key query
        key_name='conversation_id',
        key_value=conversation_id
    )
    
    if not result or 'Items' not in result:
        return []
        
    # Sort by timestamp and format items
    items = result['Items']
    sorted_items = sorted(items, key=lambda x: x.get('timestamp', ''))
    
    return [{
        'subject': item.get('subject', ''),
        'body': item.get('body', ''),
        'sender': item.get('sender', ''),
        'timestamp': item.get('timestamp', ''),
        'type': item.get('type', '')
    } for item in sorted_items]

def get_account_email(account_id: str) -> Optional[str]:
    """Get account email by account ID."""
    result = invoke_db_select(
        table_name='Users',
        index_name=None,  # Primary key query
        key_name='id',
        key_value=account_id
    )
    
    return result.get('responseEmail') if result else None

def update_thread_attributes(conversation_id: str, attributes: Dict[str, Any]) -> bool:
    """Update thread with new attributes."""
    try:
        # Convert attributes to the format expected by db-select
        update_data = {
            'conversation_id': conversation_id,
            'attributes': attributes
        }
        
        result = invoke_db_select(
            table_name='Threads',
            index_name=None,  # Primary key query
            key_name='conversation_id',
            key_value=update_data
        )
        
        return result.get('success', False) if result else False
    except Exception as e:
        logger.error(f"Error updating thread attributes: {str(e)}")
        return False
