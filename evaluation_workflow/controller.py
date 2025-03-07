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
    table_name=f"evaluation_workflow_result_{os.environ.get('CURRENT_STAGE','dev')}",
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
                if reference_id in ["drug_dosage_data_gen_workflow-2024-12-04 09:47:45.214260"]:
                    logger.info("Skipping the record for reference id : %s", reference_id)
                    continue
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
                
                if executor_result.get('output'):
                    
                    logger.info("Agent Workflow Executor Successful")
                    _save = {}
                    _save['reference_id'] = reference_id
                    _save['reference_unique_id'] = reference_unique_id
                    _save['workflow_id'] = workflow_id
                    _save['reference_identity'] = reference_identity
                    #_save['execution_results'] = executor_result.get('output').get('result').get('execution_results')
                    _save['execution_order'] = executor_result.get('output').get('result').get('execution_order')
                    _save['execution_final_output'] = executor_result.get('output').get('result').get('final_result')
                    _save['status'] = 'success'
                    _save['api_status_code'] = executor_result.get('status_code')
                    _save['api_raw_output'] = base64.b64encode(zlib.compress(bytes(executor_result.get('message'), encoding='utf-8'))).decode()
                    _save['workflow_activity_status'] = "COMPLETED"
                    _save['image_urls'] = image_urls
                    _save['input_args'] = input_args
                    _save['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                    
                    #Check for error state in validations
                    try:
                        logger.info("Saving the result in dynamo for object : %s", _save)
                        dynamo_response = dynamo_client.add_item(
                            item=validate_dynamo_save_payload(
                                obj=_save
                            )
                        )
                        logger.info("Dynamo Saved Response : %s", dynamo_response)
                    except Exception as exc:
                        logger.error("Error in validations -: %s", str(exc), exc_info=True)
                        invoke_message_to_channel(
                            message=exc
                        )
                        _save['reference_id'] = reference_id
                        _save['reference_unique_id'] = reference_unique_id
                        _save['workflow_id'] = workflow_id
                        _save['reference_identity'] = reference_identity
                        #_save['execution_results'] = executor_result.get('output').get('result').get('execution_results')
                        _save['execution_order'] = None
                        _save['execution_final_output'] = None
                        _save['status'] = 'error'
                        _save['api_status_code'] = executor_result.get('status_code')
                        _save['api_raw_output'] = base64.b64encode(zlib.compress(bytes(executor_result.get('message'), encoding='utf-8'))).decode()
                        _save['workflow_activity_status'] = "COMPLETED"
                        _save['image_urls'] = None
                        _save['input_args'] = None
                        _save['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                        dynamo_response = dynamo_client.add_item(
                            item=validate_dynamo_save_payload(
                                obj=_save
                            )
                        )
                        
                else:
                    logger.info("Agent Workflow Executor Failed")
                    _save = {}
                    _save['reference_id'] = reference_id
                    _save['reference_unique_id'] = reference_unique_id
                    _save['workflow_id'] = workflow_id
                    _save['reference_identity'] = reference_identity
                    #_save['execution_results'] = {}
                    _save['execution_order'] = {}
                    _save['execution_final_output'] = {}
                    _save['status'] = 'failed'
                    _save['api_status_code'] = executor_result.get('status_code')
                    _save['api_raw_output'] = base64.b64encode(zlib.compress(bytes(executor_result.get('message'), encoding='utf-8'))).decode()
                    _save['workflow_activity_status'] = "COMPLETED"
                    _save['input_args'] = input_args
                    _save['image_urls'] = image_urls
                    _save['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                    # try:
                    logger.info("Saving the result in dynamo for object : %s", _save)
                    dynamo_response = dynamo_client.add_item(
                        item=validate_dynamo_save_payload(
                            obj=_save
                        )
                    )
                    logger.info("Dynamo Saved Response : %s", dynamo_response)
                    #     logger.info("Successfully saved the result in dynamo for reference id : %s", reference_id)
                    # except Exception as e:
                    #     logger.error("Error while saving the result in dynamo for reference id : %s", reference_id)
                    #     logger.error("Error: %s", e, exc_info=True)
                        
            else:
                logger.error("No new image found in the record or the image is broken")
            

    # return {
    #         "status": 200,
    #         "message": "success"
    #     }