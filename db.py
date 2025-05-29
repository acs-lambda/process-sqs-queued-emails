# db.py
import boto3
from botocore.exceptions import ClientError
from config import AWS_REGION

dynamodb_resource = boto3.resource('dynamodb', region_name=AWS_REGION)


def get_conversation_id(message_id: str):
    if not message_id:
        return None
    table = dynamodb_resource.Table('Conversations')
    try:
        res = table.query(
            IndexName='response_id-index',
            KeyConditionExpression='response_id = :id',
            ExpressionAttributeValues={':id': message_id}
        )
        items = res.get('Items', [])
        return items[0]['conversation_id'] if items else None
    except Exception:
        return None


def get_associated_account(email: str):
    table = dynamodb_resource.Table('Users')
    key = 'responseEmail'
    values = {':email': email.lower()}
    try:
        res = table.query(
            IndexName=('responseEmail-index'),
            KeyConditionExpression=f"{key} = :email",
            ExpressionAttributeValues=values
        )
        print("Associated ID Response", res)
        return res['Items'][0]['id'] if res.get('Items') else None
    except Exception:
        return None


def get_email_chain(conversation_id: str):
    table = dynamodb_resource.Table('Conversations')
    res = table.query(
        KeyConditionExpression='conversation_id = :cid',
        ExpressionAttributeValues={':cid': conversation_id}
    )
    print("Fetched email chain: ", res)
    items = res.get('Items', [])
    # sort and return, omitted here for brevity
    return items


def get_account_email(account_id: str):
    table = dynamodb_resource.Table('Users')
    try:
        res = table.get_item(Key={'id': account_id})
        return res['Item']['responseEmail'] if 'Item' in res else None
    except ClientError:
        return None
