import logging
from typing import Any, Dict, List
from datetime import datetime

# Initialize logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded %s", __name__)

# Record type configurations
RECORD_TYPE_CONFIG = {
    'Weight': {
        'data_field': 'weight',
        'value_field': 'value',
        'unit_field': 'type'
    },
    'Height': {
        'data_field': 'height',
        'value_field': 'value',
        'unit_field': 'type'
    },
    'TotalCaloriesBurned': {
        'data_field': 'energy',
        'value_field': 'value',
        'unit_field': 'type'
    },
    "Distance": {
        'data_field': 'distance',
        'value_field': 'value',
        'unit_field': 'type'
    }
    # Additional record types can be added here
}

def invoke_health_transform(
    item: Dict[str, Any],
    record_type_config: Dict[str, Dict[str, str]] = RECORD_TYPE_CONFIG
) -> List[Dict[str, Any]]:
    """
    Extracts important information from the deserialized item
    and returns a list of flattened records.

    Parameters:
    - item: The input data item containing health records.
    - record_type_config: Configuration mapping for record types.

    Returns:
    - A list of transformed records.
    """
    # Extract common attributes
    user_id = item.get('user_id')
    source = item.get('source')
    record_type = item.get('type')  # e.g., "Weight", "Height", "TotalCaloriesBurned"
    dynamo_created_at = item.get('created_at')

    # Handle 'data' which is a list of entries
    data_entries = item.get('data', [])
    transformed_records = []

    # Get the configuration for the current record type
    config = record_type_config.get(record_type)
    if not config:
        logger.warning("Unsupported record type: %s", record_type)
        return transformed_records  # Return empty list

    data_field = config.get('data_field')
    value_field = config.get('value_field')
    unit_field = config.get('unit_field')

    for entry_idx, entry in enumerate(data_entries):
        # For each entry, extract necessary fields
        entry_time = entry.get('time') or entry.get('startTime')
        if not entry_time:
            logger.warning("Time field missing in entry at index %s", entry_idx)
            continue  # Skip entries without a time field

        # Extract value and unit
        data_content = entry.get(data_field, {})
        if data_content and isinstance(data_content, dict):
            value = data_content.get(value_field)
            unit = data_content.get(unit_field)
        else:
            logger.warning(
                "Missing expected data fields in entry at index %s for type %s",
                entry_idx, record_type
            )
            continue  # Skip entries that don't match the expected structure

        # Extract last modified time
        metadata = entry.get('metadata', {})
        last_modified_time = metadata.get('lastModifiedTime')

        # Construct the transformed record
        transformed_record = {
            'user_id': user_id,
            'vital_source': source,
            'entry_time': entry_time,
            'vital_type': record_type,
            'vital_value': value,
            'vital_unit': unit,
            'last_modified_time': last_modified_time,
            'dynamo_created_at':dynamo_created_at
        }

        transformed_records.append(transformed_record)
    
    return transformed_records
    