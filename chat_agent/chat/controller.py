import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

import requests
import json

def invoke_initiate_chat(
    token: str,
    user_type: str,
    env: str
):
    out = None
    url = f"https://api-server.jivihealth.org/jarvis/{env}/api/v1/chat/init"

    payload = {}
    headers = {
        'accept': '*/*',
        'token': token,
        'userType': user_type
    }

    response = requests.request(
        "POST",
        url,
        headers=headers,
        data=payload,
        timeout=30
    )

    logger.info(f"Invoked chat initiation - {response.text} and status - {response.status_code}")
    if response.status_code == 200:
        out = response.json()
    
    return out

def invoke_chat(
   session_id: str,
   message: str,
   token: str,
   user_type: str,
   env: str
):
    out = None
    url = f"https://api-server.jivihealth.org/jarvis/{env}/api/v2/chat/guided"

    payload = json.dumps({
        "session_id": session_id,
        "return_query": True,
        "meta": {},
        "symptoms": [{
            "code": [message]
        }],
        "specialist":"physician"
    })
    headers = {
        'accept': '*/*',
        'token': token,
        'userType': user_type,
        'Content-Type': 'application/json'
    }

    response = requests.request(
        "POST",
        url,
        headers=headers,
        data=payload,
        timeout=30
    )
    #logger.info(f"Invoked chat message with payload: {payload}")
    logger.info(f"Invoked chat message - {response.text} and status - {response.status_code}")
    if response.status_code == 200:
        out = response.json()
    
    return out