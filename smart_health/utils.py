import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

from boto3.dynamodb.types import TypeDeserializer
from datetime import datetime, timedelta
import pytz

def deserialize_dynamo_event(
    dynamo_record
):
    """
    Deserializes a DynamoDB event record into standard Python types.
    """
    deserializer = TypeDeserializer()

    def deserializer_helper(d):
        if isinstance(d, dict):
            if len(d) == 1 and next(iter(d)).isupper():
                # It's a DynamoDB type (e.g., {'S': 'string'})
                return deserializer.deserialize(d)
            else:
                # It's a regular dict, recursively deserialize its contents
                return {k: deserializer_helper(v) for k, v in d.items()}
        elif isinstance(d, list):
            # Deserialize each item in the list
            return [deserializer_helper(v) for v in d]
        else:
            # Base case, return the value as is
            return d

    return deserializer_helper(dynamo_record)

def parse_zone_timestamp(
    timestamp
):
    # Parse the UTC timestamp
    dt = datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
    
    # Add 5 hours and 30 minutes to convert to IST
    ist = dt + timedelta(hours=5, minutes=30)
    
    # Format the datetime in IST
    formatted_dt = ist.strftime('%Y-%m-%d %H:%M:%S')
    return formatted_dt