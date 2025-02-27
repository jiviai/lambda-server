import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

import json
import requests

def invoke_workflow_executor(
    workflow_id: str,
    input_args: dict,
    image_urls: dict
):
    url = "https://api-server.jivihealth.org/ds/api/agent_workflow/v1/invoke_workflow"

    payload = {
        'workflow_id': workflow_id,
        'input_args': json.dumps(input_args),
        'image_urls': json.dumps(image_urls)
    }
    files = []
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Authorization': 'Basic ZHNfdXNlcjpkc0AxMjM=',
        'debug': 'true'
    }

    response = requests.request(
        "POST",
        url,
        headers=headers,
        data=payload,
        files=files,
        timeout=300
    )
    logger.info(f"Response from workflow invocation - {response.text} with status - {response.status_code}")

    if response.status_code == 200:
        return {
            "output":response.json(),
            "status_code":response.status_code,
            "message":response.text
        }
    
    return {
        "output":None,
        "status_code":response.status_code,
        "message":response.text
    }