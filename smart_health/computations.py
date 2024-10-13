import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded %s", __name__)

import os

from smart_health.utils import parse_zone_timestamp, convert_to_ist

BASELINE_MIN_HRV = int(os.environ.get('BASELINE_MIN_HRV', 100))
BASELINE_MAX_HRV = int(os.environ.get('BASELINE_MAX_HRV', 1000))
BASELINE_MIN_BPM = int(os.environ.get('BASELINE_MIN_BPM', 50))
BASELINE_MAX_BPM = int(os.environ.get('BASELINE_MAX_BPM', 150))

def calculate_stress_score(hrv_value, min_hrv=BASELINE_MIN_HRV, max_hrv=BASELINE_MAX_HRV):
    """
    Calculates the stress score based on HRV.
    Lower HRV results in higher stress scores.
    """
    if not hrv_value or hrv_value <= 0:
        return 95.0  # Return maximum stress if input is invalid

    # Allow HRV values to exceed the specified boundaries
    # Normalize HRV to a value between 0 and 1
    hrv_range = max_hrv - min_hrv
    normalized_hrv = (hrv_value - min_hrv) / hrv_range

    # Invert and clamp normalized HRV
    inverted_hrv = 1.0 - normalized_hrv
    inverted_hrv = max(0.0, min(1.0, inverted_hrv))

    # Scale inverted HRV to the range of 5 to 95
    stress_score = inverted_hrv * 90 + 5
    stress_score = max(5.0, min(95.0, stress_score))

    return round(stress_score, 2)

def calculate_recovery_score(hrv_value, min_hrv=BASELINE_MIN_HRV, max_hrv=BASELINE_MAX_HRV):
    """
    Calculates the recovery score based on HRV.
    Higher HRV results in higher recovery scores.
    """
    if not hrv_value or hrv_value <= 0:
        return 5.0  # Return minimum recovery if input is invalid

    # Allow HRV values to exceed the specified boundaries
    # Normalize HRV to a value between 0 and 1
    hrv_range = max_hrv - min_hrv
    normalized_hrv = (hrv_value - min_hrv) / hrv_range

    # Clamp normalized HRV
    normalized_hrv = max(0.0, min(1.0, normalized_hrv))

    # Scale normalized HRV to the range of 5 to 95
    recovery_score = normalized_hrv * 90 + 5
    recovery_score = max(5.0, min(95.0, recovery_score))

    return round(recovery_score, 2)

def calculate_energy_score(bpm, min_bpm=BASELINE_MIN_BPM, max_bpm=BASELINE_MAX_BPM):
    """
    Calculates the energy score based on heart rate.
    Higher heart rates correspond to higher energy levels.
    """
    if bpm is None or bpm <= 0:
        return 5.0  # Return minimum energy if input is invalid

    # Allow BPM values to exceed the specified boundaries
    # Normalize BPM to a value between 0 and 1
    bpm_range = max_bpm - min_bpm
    normalized_bpm = (bpm - min_bpm) / bpm_range

    # Clamp normalized BPM
    normalized_bpm = max(0.0, min(1.0, normalized_bpm))

    # Scale normalized BPM to the range of 5 to 95
    energy_score = normalized_bpm * 90 + 5
    energy_score = max(5.0, min(95.0, energy_score))

    return round(energy_score, 2)

def calculates_stress_from_hrv(
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
        parsed_record['score'] = calculate_stress_score(float(record.get('heartRateVariabilityMillis')))
        parsed_record['last_modified_time'] = record.get('metadata').get('lastModifiedTime')
        parsed_record['recorded_time'] = parse_zone_timestamp(record.get('metadata').get('lastModifiedTime'))
        parsed_record["activity_start_time"] = convert_to_ist(start_time)
        parsed_record["activity_end_time"] = convert_to_ist(record.get('endTime'))
        transformed_records.append(parsed_record)
        
    return transformed_records

def calculates_recovery_from_hrv(
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
        parsed_record['score'] = calculate_recovery_score(float(record.get('heartRateVariabilityMillis')))
        parsed_record['last_modified_time'] = record.get('metadata').get('lastModifiedTime')
        parsed_record['recorded_time'] = parse_zone_timestamp(record.get('metadata').get('lastModifiedTime'))
        parsed_record["activity_start_time"] = convert_to_ist(start_time)
        parsed_record["activity_end_time"] = convert_to_ist(record.get('endTime'))
        transformed_records.append(parsed_record)
        
    return transformed_records

def calculates_energy_from_hr(
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
            parsed_record['score'] = calculate_energy_score(float(sample.get('beatsPerMinute')))
            parsed_record['last_modified_time'] = record.get('metadata').get('lastModifiedTime')
            parsed_record['recorded_time'] = parse_zone_timestamp(record.get('metadata').get('lastModifiedTime'))
            parsed_record["activity_start_time"] = convert_to_ist(record.get('startTime'))
            parsed_record["activity_end_time"] = convert_to_ist(record.get('endTime'))
            transformed_records.append(parsed_record)
        
    return transformed_records