import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

from boto3.dynamodb.types import TypeDeserializer
from datetime import datetime, timedelta
import pytz

def remove_duplicates(data_list, conflict_columns):
    try:
        seen = set()
        unique_data_list = []

        for record in data_list:
            record_key = tuple(record[col] for col in conflict_columns)
            if record_key not in seen:
                seen.add(record_key)
                unique_data_list.append(record)

        return unique_data_list
    except Exception as e:
        logger.error(f"Error removing duplicates: {e}")
        return data_list
    
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
    try:
        parsers_list = [
            '%Y-%m-%dT%H:%M:%S.%fZ',  # For strings with milliseconds
            '%Y-%m-%dT%H:%M:%SZ',# For strings without milliseconds
            '%Y-%m-%d %H:%M:%S',  # For standard
            '%Y-%m-%d %H:%M:%S.%f',  # For standard with ms
        ]
        
        # Try each format in parsers_list to parse the input ISO format string
        for parser in parsers_list:
            try:
                # Parse the input ISO string to a UTC datetime object
                dt_utc = datetime.strptime(timestamp, parser)
                break  # Exit the loop if successful
            except ValueError:
                continue  # Try the next format if parsing fails
        else:
            # Raise an exception if no format matches
            raise ValueError(f"Invalid datetime format parser: {timestamp}")
        
        #India Standard Time (IST) is UTC+5:30
        ist_offset = timedelta(hours=5, minutes=30)
        
        # Add the IST offset to the UTC time
        dt_ist = dt_utc + ist_offset
        
        # Format the IST datetime to the desired format 'YYYY-MM-DD HH:MM:SS'
        return dt_ist.strftime('%Y-%m-%d %H:%M:%S')
    
    except Exception as e:
        logger.error(f"Error converting datetime format: {e} {timestamp}")
        return None

def convert_to_ist(iso_string: str) -> str:
    try:
        parsers_list = [
            '%Y-%m-%dT%H:%M:%S.%fZ',  # For strings with milliseconds
            '%Y-%m-%dT%H:%M:%SZ',# For strings without milliseconds
            '%Y-%m-%d %H:%M:%S',  # For standard
            '%Y-%m-%d %H:%M:%S.%f',  # For standard with ms
        ]
        
        # Try each format in parsers_list to parse the input ISO format string
        for parser in parsers_list:
            try:
                # Parse the input ISO string to a UTC datetime object
                dt_utc = datetime.strptime(iso_string, parser)
                break  # Exit the loop if successful
            except ValueError:
                continue  # Try the next format if parsing fails
        else:
            # Raise an exception if no format matches
            raise ValueError(f"Invalid ISO format: {iso_string}")
        
        # India Standard Time (IST) is UTC+5:30
        # ist_offset = timedelta(hours=5, minutes=30)
        
        # # Add the IST offset to the UTC time
        # dt_ist = dt_utc + ist_offset
        
        # Format the IST datetime to the desired format 'YYYY-MM-DD HH:MM:SS'
        return dt_utc.strftime('%Y-%m-%d %H:%M:%S')
    
    except Exception as e:
        logger.error(f"Error converting to IST: {e} {iso_string}")
        return None
    
def deduplicate_by_keys(
    dict_list,
    keys
):
    if len(keys) == 0:
        return dict_list

    seen = {}
    for d in reversed(dict_list):
        key = tuple(d[k] for k in keys)
        if key not in seen:
            seen[key] = d

    return list(reversed(seen.values()))