import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded %s", __name__)

from smart_health.utils import parse_zone_timestamp, convert_to_ist

def transform_heart_rate_variability(
    item
):
    #Global Start and End Time
    #Sample 01j8tthyp95gc82tjwm9x8p0mc 01HSDE3Q8W6XNN2C6ZM3A4A1JW
    transformed_records = []
    
    user_id = item.get('user_id')
    dynamo_created_at = item.get('created_at')
    start_time = item.get('start_time')
    end_time = item.get('end_time')
    source = item.get('source')
    
    data = item.get('data', [])
    
    if len(data) == 0:
        logger.warning("No data found for user_id: %s", user_id)
        return transformed_records
    
    for record in data:
        parsed_record = {}
        parsed_record['user_id'] = user_id
        parsed_record['source'] = source
        parsed_record['start_time'] = start_time
        parsed_record['end_time'] = end_time
        parsed_record['dynamo_created_at'] = dynamo_created_at
        parsed_record['heart_rate_variability'] = record.get('heartRateVariabilityMillis')
        parsed_record['last_modified_time'] = record.get('metadata').get('lastModifiedTime')
        parsed_record['recorded_time'] = parse_zone_timestamp(record.get('metadata').get('lastModifiedTime'))
        parsed_record["activity_start_time"] = convert_to_ist(start_time)
        parsed_record["activity_end_time"] = convert_to_ist(end_time)
        transformed_records.append(parsed_record)
        
    return transformed_records

def transform_heart_rate(
    item
):
    transformed_records = []
    
    user_id = item.get('user_id')
    dynamo_created_at = item.get('created_at')
    start_time = item.get('start_time')
    end_time = item.get('end_time')
    source = item.get('source')
    
    data = item.get('data', [])
    
    if len(data) == 0:
        logger.warning("No data found for user_id: %s", user_id)
        return transformed_records
    
    for record in data:
        for sample in record.get('samples'):
            parsed_record = {}
            parsed_record['user_id'] = user_id
            parsed_record['source'] = source
            parsed_record['start_time'] = start_time
            parsed_record['end_time'] = end_time
            parsed_record['dynamo_created_at'] = dynamo_created_at
            parsed_record['beats_per_minute'] = sample.get('beatsPerMinute')
            parsed_record['last_modified_time'] = record.get('metadata').get('lastModifiedTime')
            parsed_record['recorded_time'] = parse_zone_timestamp(record.get('metadata').get('lastModifiedTime'))
            parsed_record["activity_start_time"] = convert_to_ist(record.get('startTime'))
            parsed_record["activity_end_time"] = convert_to_ist(record.get('endTime'))
            transformed_records.append(parsed_record)
        
    return transformed_records

def transform_steps(
    item
):
    transformed_records = []
    
    user_id = item.get('user_id')
    dynamo_created_at = item.get('created_at')
    start_time = item.get('start_time')
    end_time = item.get('end_time')
    source = item.get('source')
    
    data = item.get('data', [])
    
    if len(data) == 0:
        logger.warning("No data found for user_id: %s", user_id)
        return transformed_records
    
    for record in data:
        parsed_record = {}
        parsed_record['user_id'] = user_id
        parsed_record['source'] = source
        parsed_record['start_time'] = start_time
        parsed_record['end_time'] = end_time
        parsed_record['dynamo_created_at'] = dynamo_created_at
        parsed_record['steps'] = float(record.get('count'))
        parsed_record['last_modified_time'] = record.get('metadata').get('lastModifiedTime')
        parsed_record['recorded_time'] = parse_zone_timestamp(record.get('metadata').get('lastModifiedTime'))
        parsed_record["activity_start_time"] = convert_to_ist(record.get('startTime'))
        parsed_record["activity_end_time"] = convert_to_ist(record.get('endTime'))
        transformed_records.append(parsed_record)
        
    return transformed_records

def transform_distance(
    item
):
    transformed_records = []
    
    user_id = item.get('user_id')
    dynamo_created_at = item.get('created_at')
    start_time = item.get('start_time')
    end_time = item.get('end_time')
    source = item.get('source')
    
    data = item.get('data', [])
    
    if len(data) == 0:
        logger.warning("No data found for user_id: %s", user_id)
        return transformed_records
    
    for record in data:
        parsed_record = {}
        parsed_record['user_id'] = user_id
        parsed_record['source'] = source
        parsed_record['start_time'] = start_time
        parsed_record['end_time'] = end_time
        parsed_record['dynamo_created_at'] = dynamo_created_at
        parsed_record['distance'] = float(record.get('distance').get('value'))
        parsed_record['last_modified_time'] = record.get('metadata').get('lastModifiedTime')
        parsed_record['recorded_time'] = parse_zone_timestamp(record.get('metadata').get('lastModifiedTime'))
        parsed_record["activity_start_time"] = convert_to_ist(record.get('startTime'))
        parsed_record["activity_end_time"] = convert_to_ist(record.get('endTime'))
        transformed_records.append(parsed_record)
        
    return transformed_records

def transform_floors_climbed(
    item
):
    transformed_records = []
    
    user_id = item.get('user_id')
    dynamo_created_at = item.get('created_at')
    start_time = item.get('start_time')
    end_time = item.get('end_time')
    source = item.get('source')
    
    data = item.get('data', [])
    
    if len(data) == 0:
        logger.warning("No data found for user_id: %s", user_id)
        return transformed_records
    
    for record in data:
        parsed_record = {}
        parsed_record['user_id'] = user_id
        parsed_record['source'] = source
        parsed_record['start_time'] = start_time
        parsed_record['end_time'] = end_time
        parsed_record['dynamo_created_at'] = dynamo_created_at
        parsed_record['floors'] = float(record.get('floors'))
        parsed_record['last_modified_time'] = record.get('metadata').get('lastModifiedTime')
        parsed_record['recorded_time'] = parse_zone_timestamp(record.get('metadata').get('lastModifiedTime'))
        parsed_record["activity_start_time"] = convert_to_ist(record.get('startTime'))
        parsed_record["activity_end_time"] = convert_to_ist(record.get('endTime'))
        transformed_records.append(parsed_record)
        
    return transformed_records

def transform_total_calories_burned(
    item
):
    transformed_records = []
    
    user_id = item.get('user_id')
    dynamo_created_at = item.get('created_at')
    start_time = item.get('start_time')
    end_time = item.get('end_time')
    source = item.get('source')
    
    data = item.get('data', [])
    
    if len(data) == 0:
        logger.warning("No data found for user_id: %s", user_id)
        return transformed_records
    
    for record in data:
        parsed_record = {}
        parsed_record['user_id'] = user_id
        parsed_record['source'] = source
        parsed_record['start_time'] = start_time
        parsed_record['end_time'] = end_time
        parsed_record['dynamo_created_at'] = dynamo_created_at
        parsed_record['calories'] = float(record.get('energy').get('value'))
        parsed_record['last_modified_time'] = record.get('metadata').get('lastModifiedTime')
        parsed_record['recorded_time'] = parse_zone_timestamp(record.get('metadata').get('lastModifiedTime'))
        parsed_record["activity_start_time"] = convert_to_ist(record.get('startTime'))
        parsed_record["activity_end_time"] = convert_to_ist(record.get('endTime'))
        transformed_records.append(parsed_record)
        
    return transformed_records

def transform_resting_heart_rate(
    item
):
    transformed_records = []
    
    user_id = item.get('user_id')
    dynamo_created_at = item.get('created_at')
    start_time = item.get('start_time')
    end_time = item.get('end_time')
    source = item.get('source')
    
    data = item.get('data', [])
    
    if len(data) == 0:
        logger.warning("No data found for user_id: %s", user_id)
        return transformed_records
    
    for record in data:
        parsed_record = {}
        parsed_record['user_id'] = user_id
        parsed_record['source'] = source
        parsed_record['start_time'] = start_time
        parsed_record['end_time'] = end_time
        parsed_record['dynamo_created_at'] = dynamo_created_at
        parsed_record['beats_per_minute'] = record.get('beatsPerMinute')
        parsed_record['last_modified_time'] = record.get('metadata').get('lastModifiedTime')
        parsed_record['recorded_time'] = parse_zone_timestamp(record.get('metadata').get('lastModifiedTime'))
        parsed_record["activity_start_time"] = convert_to_ist(record.get('startTime'))
        parsed_record["activity_end_time"] = convert_to_ist(record.get('endTime'))
        transformed_records.append(parsed_record)
        
    return transformed_records