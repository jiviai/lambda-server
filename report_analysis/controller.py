import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

import zlib, base64

from datetime import datetime
import json
import os

from evaluation_workflow.api.api import invoke_workflow_executor
from evaluation_workflow.api.slack import invoke_message_to_channel
from evaluation_workflow.utils import validate_dynamo_save_payload
from evaluation_workflow.db import DynamoDBClient

dynamo_client = DynamoDBClient(
    table_name=f"evaluation_workflow_result_{os.environ.get('DEPLOYMENT_ENV','dev')}",
    region_name='ap-south-1'
)

def lambda_handler(
    event,
    context
):
    