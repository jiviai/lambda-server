import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

import os
from datetime import datetime

from smart_health.utils import deserialize_dynamo_event, deduplicate_by_keys, read_json_as_dict_from_s3
#from smart_health.parser import invoke_health_transform
from smart_health.db import PostgresDBHandler
from smart_health.processor import (
    transform_heart_rate_variability,
    transform_heart_rate,
    transform_resting_heart_rate,
    transform_steps,
    transform_distance,
    transform_total_calories_burned,
    transform_floors_climbed
)
from smart_health.computations import (
    calculates_stress_from_hrv,
    calculates_recovery_from_hrv,
    calculates_energy_from_hr
)

db_handler = PostgresDBHandler(
    host=os.environ.get('postgres_host'),
    database=os.environ.get('postgres_db'),
    user=os.environ.get('postgres_user'),
    password=os.environ.get('postgres_password')
)
DB_DEDUPE_ENABLE = bool(os.environ.get('db_dedupe_enable', 0))

def lambda_handler(
    event,
    context
):
    
    for record in event['Records']:
        logger.info("Processing record: %s", record)

        #For S3 PUT Event
        if record.get('eventSource') == 'aws:s3':
            logger.info("Processing S3 event")
            
            #Process Only PUT events
            if record.get('eventName') == 'ObjectCreated:Put':
                s3_record = record.get('s3', {})
                bucket = s3_record.get('bucket', {}).get('name')
                key = s3_record.get('object', {}).get('key')
                
                if key:
                    data_value_dict = read_json_as_dict_from_s3(
                        bucket=bucket,
                        key=key
                    )
                    parts = key.split('/')
                    s3_ingestion_date = parts[2]
                    user_id = parts[3].split('_')[1]
                    source = data_value_dict.get('metadata').get('source')
                    record_type = data_value_dict.get('metadata').get('type')
                    logger.info("Data received for user_id: %s, source: %s, type: %s, ingestion_date: %s", user_id, source, record_type, s3_ingestion_date)
                    
                    serialized_dict = {
                        "user_id": user_id,
                        "source": source,
                        "created_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "start_time": data_value_dict.get('metadata').get('start_time'),
                        "end_time": data_value_dict.get('metadata').get('end_time'),
                        "data": data_value_dict.get('data')
                    }
                                        
                    if record_type == 'HeartRateVariabilityRmssd':
                        transformed_records = transform_heart_rate_variability(
                            item=serialized_dict
                        )
                        
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'heart_rate_variability',
                            'last_modified_time',
                            'recorded_time',
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s with size %s", transformed_records, len(transformed_records))
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('heart_rate_variability_rmssd', transformed_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()
                        
                        #For calculated parameters
                        stress_records = calculates_stress_from_hrv(
                            item=serialized_dict
                        )
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'score',
                            'last_modified_time',
                            'recorded_time',
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s with size %s", transformed_records, len(transformed_records))
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('stress', stress_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()
                        
                        #Recovery Calculation
                        recovery_records = calculates_recovery_from_hrv(
                            item=serialized_dict
                        )
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'score',
                            'last_modified_time',
                            'recorded_time',
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s with size %s", transformed_records, len(transformed_records))
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('recovery', recovery_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()

                        
                    if record_type == 'HeartRate':
                        transformed_records = transform_heart_rate(
                            item=serialized_dict
                        )
                        
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'beats_per_minute',
                            'last_modified_time',
                            'recorded_time',
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s", transformed_records)
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('heart_rate', transformed_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()
                        
                        #Energy Calculation
                        energy_records = calculates_energy_from_hr(
                            item=serialized_dict
                        )
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'score',
                            'last_modified_time',
                            'recorded_time',
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s with size %s", transformed_records, len(transformed_records))
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('energy', energy_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()
                    
                    if record_type == 'RestingHeartRate':
                        transformed_records = transform_resting_heart_rate(
                            item=serialized_dict
                        )
                        
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'beats_per_minute',
                            'last_modified_time',
                            'recorded_time',
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s", transformed_records)
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('resting_heart_rate', transformed_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()
                        
                    if record_type == 'Steps':
                        transformed_records = transform_steps(
                            item=serialized_dict
                        )
                        
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'steps',
                            'last_modified_time',
                            'recorded_time',
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s", transformed_records)
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('steps', transformed_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()
                        
                    if record_type == 'Distance':
                        transformed_records = transform_distance(
                            item=serialized_dict
                        )
                        
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'distance',
                            'last_modified_time',
                            'recorded_time',
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s", transformed_records)
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('distance', transformed_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()
                        
                    if record_type == 'TotalCaloriesBurned':
                        transformed_records = transform_total_calories_burned(
                            item=serialized_dict
                        )
                        
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'calories',
                            'last_modified_time',
                            'recorded_time',  
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s", transformed_records)
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('total_calories_burned', transformed_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()
                        
                    if record_type == 'FloorsClimbed':
                        transformed_records = transform_floors_climbed(
                            item=serialized_dict
                        )
                        
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'floors',
                            'last_modified_time',
                            'recorded_time',
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s", transformed_records)
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('floors_climbed', transformed_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()
                
        #For Dynamo INSERT Event
        if record.get('eventSource') == 'aws:dynamodb':
            logger.info("Processing DynamoDB event")
            
            # Process only 'INSERT' events
            if record.get('eventName') == 'INSERT':
                dynamodb_record = record.get('dynamodb', {})
                new_image = dynamodb_record.get('NewImage', {})
                
                if new_image:
                    serialized_dynamo_dict = deserialize_dynamo_event(
                        dynamo_record=new_image
                    )
                    user_id = serialized_dynamo_dict.get('user_id')
                    source = serialized_dynamo_dict.get('source')
                    record_type = serialized_dynamo_dict.get('type')
                    logger.info("Data received for user_id: %s, source: %s, type: %s", user_id, source, record_type)

                    if record_type == 'HeartRateVariabilityRmssd':
                        transformed_records = transform_heart_rate_variability(
                            item=serialized_dynamo_dict
                        )
                        
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'heart_rate_variability',
                            'last_modified_time',
                            'recorded_time',
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s with size %s", transformed_records, len(transformed_records))
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('heart_rate_variability_rmssd', transformed_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()
                        
                        #For calculated parameters
                        stress_records = calculates_stress_from_hrv(
                            item=serialized_dynamo_dict
                        )
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'score',
                            'last_modified_time',
                            'recorded_time',
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s with size %s", transformed_records, len(transformed_records))
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('stress', stress_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()
                        
                        #Recovery Calculation
                        recovery_records = calculates_recovery_from_hrv(
                            item=serialized_dynamo_dict
                        )
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'score',
                            'last_modified_time',
                            'recorded_time',
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s with size %s", transformed_records, len(transformed_records))
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('recovery', recovery_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()

                        
                    if record_type == 'HeartRate':
                        transformed_records = transform_heart_rate(
                            item=serialized_dynamo_dict
                        )
                        
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'beats_per_minute',
                            'last_modified_time',
                            'recorded_time',
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s", transformed_records)
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('heart_rate', transformed_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()
                        
                        #Energy Calculation
                        energy_records = calculates_energy_from_hr(
                            item=serialized_dynamo_dict
                        )
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'score',
                            'last_modified_time',
                            'recorded_time',
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s with size %s", transformed_records, len(transformed_records))
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('energy', energy_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()
                    
                    if record_type == 'RestingHeartRate':
                        transformed_records = transform_resting_heart_rate(
                            item=serialized_dynamo_dict
                        )
                        
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'beats_per_minute',
                            'last_modified_time',
                            'recorded_time',
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s", transformed_records)
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('resting_heart_rate', transformed_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()
                        
                    if record_type == 'Steps':
                        transformed_records = transform_steps(
                            item=serialized_dynamo_dict
                        )
                        
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'steps',
                            'last_modified_time',
                            'recorded_time',
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s", transformed_records)
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('steps', transformed_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()
                        
                    if record_type == 'Distance':
                        transformed_records = transform_distance(
                            item=serialized_dynamo_dict
                        )
                        
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'distance',
                            'last_modified_time',
                            'recorded_time',
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s", transformed_records)
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('distance', transformed_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()
                        
                    if record_type == 'TotalCaloriesBurned':
                        transformed_records = transform_total_calories_burned(
                            item=serialized_dynamo_dict
                        )
                        
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'calories',
                            'last_modified_time',
                            'recorded_time',  
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s", transformed_records)
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('total_calories_burned', transformed_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()
                        
                    if record_type == 'FloorsClimbed':
                        transformed_records = transform_floors_climbed(
                            item=serialized_dynamo_dict
                        )
                        
                        if len(transformed_records) == 0:
                            logger.warning("No records found for user_id: %s", user_id)
                            return
                        
                        columns = [
                            'user_id',
                            'source',
                            'start_time',
                            'end_time',
                            'dynamo_created_at',
                            'floors',
                            'last_modified_time',
                            'recorded_time',
                            'activity_start_time',
                            'activity_end_time'
                        ]
                        conflict_columns = None
                        logger.info("Transformed records: %s", transformed_records)
                        
                        if DB_DEDUPE_ENABLE is True:
                            transformed_records = deduplicate_by_keys(
                                dict_list=transformed_records,
                                keys=['source', 'activity_end_time']
                            )
                            logger.info("Deduplicated records: %s with size %s", transformed_records, len(transformed_records))
                        
                        db_handler.connect()
                        db_handler.write_many('floors_climbed', transformed_records, columns, conflict_columns=["activity_end_time"])
                        db_handler.disconnect()


                
