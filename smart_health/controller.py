import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

import psycopg2
import json
from boto3.dynamodb.types import TypeDeserializer

def deserialize_dynamodb_item(item):
    deserializer = TypeDeserializer()

    def deserialize(value):
        if isinstance(value, dict):
            if len(value) == 1 and list(value.keys())[0] in deserializer._dispatch:
                # It must be a DynamoDB type
                return deserializer.deserialize(value)
            else:
                # It's a dict with multiple keys
                return {k: deserialize(v) for k, v in value.items()}
        elif isinstance(value, list):
            return [deserialize(v) for v in value]
        else:
            return value

    return deserialize(item)

def lambda_handler(
    event,
    context
):
    for record in event['Records']:
        logger.info(f"Row for dynamo: {record}")
        
        if record['eventName'] == 'INSERT':
            new_image = record.get('dynamodb').get('NewImage')

            if new_image is not None:
                user_id = new_image.get('user_id').get('S')