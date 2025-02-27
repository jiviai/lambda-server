from modules.logger import setup_logger
logger = setup_logger(__name__)

from datetime import datetime
import json
import os

# from evaluation_workflow.api.api import invoke_workflow_executor
# from evaluation_workflow.api.slack import invoke_message_to_channel
# from evaluation_workflow.utils import validate_dynamo_save_payload
# from evaluation_workflow.db import DynamoDBClient

# dynamo_client = DynamoDBClient(
#     table_name=f"report_analysis_result_{os.environ.get('CURRENT_STAGE','dev')}",
#     region_name='ap-south-1'
# )

def lambda_handler(
    event,
    context
):
    logger.info(f"Report Analysis Payload - {event} and Context - {context}")  
    # for record in event['Records']:
    #     logger.info(f"Row for dynamo: {record}")
        
    #     if record['eventName'] == 'INSERT':
    #         new_image = record.get('dynamodb').get('NewImage')

    # return {
    #         "status": 200,
    #         "message": "success"
    #     }