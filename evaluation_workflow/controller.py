import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

from datetime import datetime
import json

from evaluation_workflow.api.api import invoke_workflow_executor
from evaluation_workflow.utils import validate_dynamo_save_payload
from evaluation_workflow.db import DynamoDBClient

dynamo_client = DynamoDBClient(
    table_name='evaluation_workflow_result_dev',
    region_name='ap-south-1'
)

def lambda_handler(
    event,
    context
):        
    for record in event['Records']:
        logger.info(f"Row for dynamo: {record}")
        
        if record['eventName'] == 'INSERT':
            new_image = record.get('dynamodb').get('NewImage')

            if new_image is not None:
                reference_id = new_image.get('reference_id').get('S')
                reference_unique_id = new_image.get('reference_unique_id').get('S')
                reference_identity = new_image.get('reference_identity').get('N')
                workflow_id = new_image.get('workflow_id').get('S')
                input_args = json.loads(new_image.get('input_args').get('S'))
                image_urls = json.loads(new_image.get('image_urls').get('S'))
                
                logger.info("Input Args: %s Image URLS %s", input_args, image_urls)
                                
                #Invoke Executor API
                executor_result = invoke_workflow_executor(
                    workflow_id=workflow_id,
                    input_args=input_args,
                    image_urls=image_urls
                )
                
                if executor_result:
                    _save = {}
                    _save['reference_id'] = reference_id
                    _save['reference_unique_id'] = reference_unique_id
                    _save['workflow_id'] = workflow_id
                    _save['reference_identity'] = reference_identity
                    _save['execution_results'] = executor_result.get('result').get('execution_results')
                    _save['execution_order'] = executor_result.get('result').get('execution_order')
                    _save['status'] = 'success'
                    _save['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                    dynamo_client.add_item(
                        item=validate_dynamo_save_payload(
                            obj=_save
                        )
                    )
                    logger.info("Successfully saved the result in dynamo for reference id : %s", reference_id)
            else:
                logger.error("No new image found in the record or the image is broken")
            

    # return {
    #         "status": 200,
    #         "message": "success"
    #     }