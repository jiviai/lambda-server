import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

import requests

def invoke_user_token_generate(
    env: str
):
    out = None
    url = f"https://api-server.jivihealth.org/jarvis/{env}/api/v1/anonymous/token/generate"

    payload = {}
    headers = {
        'accept': '*/*'
    }

    response = requests.request(
        "POST", 
        url,
        headers=headers,
        data=payload,
        timeout=30
    )
    logger.info(f"Invoked user and token generate with - {response.text} and status - {response.status_code}")
    if response.status_code == 200:
        out = response.json()
    
    return out