import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

import json
import requests
import os

AGENT_FRAMEWORK_URL = os.environ.get("AGENT_FRAMEWORK_URL", "https://api-server.jivihealth.org/ds/api/framework/uat")
AGENT_FRAMEWORK_AUTH = os.environ.get("AGENT_FRAMEWORK_AUTH", "Basic ZHNfdXNlcjpkc0AxMjM=")

CONTENT_AGENT_FRAMEWORK_MODEL_NAME = os.environ.get("CONTENT_AGENT_FRAMEWORK_MODEL_NAME", "gpt-4o-mini")
CONTENT_AGENT_FRAMEWORK_MODEL_PROVIDER = os.environ.get("CONTENT_AGENT_FRAMEWORK_MODEL_PROVIDER", "openai")

def invoke_content_creation_agent(
    agent_name: str,
    query: str,
    entity: str,
    topic: str
):
  try:
    url = f"{AGENT_FRAMEWORK_URL}/v1/agent"
    
    payload = json.dumps({
      "agent_name": agent_name,
      "input_args": {
        "query": query,
        "entity": entity,
        "topic": topic
      },
      "model_name": CONTENT_AGENT_FRAMEWORK_MODEL_NAME,
      "model_provider": CONTENT_AGENT_FRAMEWORK_MODEL_PROVIDER,
      "chat": False
    })
    
    headers = {
      'accept': 'application/json',
      'Content-Type': 'application/json',
      'Authorization': AGENT_FRAMEWORK_AUTH

    }

    response = requests.request("POST", url, headers=headers, data=payload, timeout=30)
    logger.info("Response from classification agent - %s", response.text)

    if response.status_code == 200:
        out = response.json()
        return out.get('result').get('content')
    
    return None
  except Exception as e:
    logger.error("Exception in invoking content creation agent framework - %s", str(e), exc_info=True)
    return None