from modules.logger import setup_logger
logger = setup_logger(__name__)

# from datetime import datetime
# import json
# import os

# dynamo_client = DynamoDBClient(
#     table_name=f"evaluation_workflow_result_{os.environ.get('DEPLOYMENT_ENV','dev')}",
#     region_name='ap-south-1'
# )

def lambda_handler(
    event,
    context
):
    logger.info(f"Report Analysis Event: {event} and Context: {context}")
    
    