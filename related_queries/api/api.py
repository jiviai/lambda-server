import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

import json
import requests
import os

EMBEDDING_SERVER_URL = os.environ.get("EMBEDDING_SERVER_URL", "https://api-server-stage.jivihealth.org/ds/api/utils/v1/embeddings")
EMBEDDING_SERVER_AUTH = os.environ.get("EMBEDDING_SERVER_AUTH", "Basic ZHNfdXNlcjpkc0AxMjM=")

MED_CLASSIFIER_SERVER_URL = os.environ.get("MED_CLASSIFIER_SERVER_URL", "https://api-server.jivihealth.org/ds/api/med_classifier")
MED_CLASSIFIER_SERVER_AUTH = os.environ.get("MED_CLASSIFIER_SERVER_AUTH", "Basic ZHNfdXNlcjpkc0AxMjM=")

AGENT_FRAMEWORK_URL = os.environ.get("AGENT_FRAMEWORK_URL", "https://api-server.jivihealth.org/ds/api/framework/uat")
AGENT_FRAMEWORK_AUTH = os.environ.get("AGENT_FRAMEWORK_AUTH", "Basic ZHNfdXNlcjpkc0AxMjM=")

RELATED_QUERIES_AGENT_FRAMEWORK_MODEL_NAME = os.environ.get("RELATED_QUERIES_AGENT_FRAMEWORK_MODEL_NAME", "gpt-4o-mini")
RELATED_QUERIES_AGENT_FRAMEWORK_MODEL_PROVIDER = os.environ.get("RELATED_QUERIES_AGENT_FRAMEWORK_MODEL_PROVIDER", "openai")

def invoke_embedding(
  text: str,
  model_name: str
):
  try:
    url = EMBEDDING_SERVER_URL

    payload = json.dumps({
      "text": [text],
      "modelName": model_name
    })
    headers = {
      'Content-Type': 'application/json',
      'Authorization': EMBEDDING_SERVER_AUTH
    }

    response = requests.request("POST", url, headers=headers, data=payload, timeout=30)
    logger.info("Response from embedding server - %s", response.text)

    if response.status_code == 200:
      return response.json()
    
    return None
  except Exception as e:
      logger.error("Exception in invoking embedding server details - %s", str(e), exc_info=True)
      return None
  
def invoke_related_queries_agent(
    agent_name: str,
    query: str
):
    try:
        url = f"{AGENT_FRAMEWORK_URL}/v1/agent"

        payload = json.dumps({
            "agent_name": agent_name,
            "input_args": {
                "query": query
            },
            "model_name": RELATED_QUERIES_AGENT_FRAMEWORK_MODEL_NAME,
            "model_provider": RELATED_QUERIES_AGENT_FRAMEWORK_MODEL_PROVIDER,
            "chat": False
        })

        headers = {
            'accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': AGENT_FRAMEWORK_AUTH

        }

        response = requests.request("POST", url, headers=headers, data=payload, timeout=30)
        logger.info("Response from related queries agent - %s", response.text)

        if response.status_code == 200:
            out = response.json()
            return out.get('result').get('queries')

        return None
    except Exception as e:
        logger.error("Exception in invoking related queries agent framework - %s", str(e), exc_info=True)
        return None
  
def invoke_language_translation_framework(
    language,
    obj,
    ignore_keys
):
  try:
    url = f"{AGENT_FRAMEWORK_URL}/v1/translate_json?ignore_keys={ignore_keys}"
    
    payload = json.dumps(obj)
    
    headers = {
      'accept': 'application/json',
      'Content-Type': 'application/json',
      'language': language,
      'Authorization': AGENT_FRAMEWORK_AUTH
    }

    response = requests.request("POST", url, headers=headers, data=payload, timeout=30)
    logger.info("Response from translation framework - %s", response.text)

    if response.status_code == 200:
        out = response.json()
        return out.get('result').get('translated')
    
    return None
  except Exception as e:
    logger.error("Exception in invoking translation framework - %s", str(e), exc_info=True)
    return None
  
def invoke_search_api(
  query: str,
  language: str
):
  try:
    url = f"{MED_CLASSIFIER_SERVER_URL}/v1/classify_query"

    payload = json.dumps({
      "user_id": "content-user-id",
      "query": query
    })
    headers = {
      'language': language,
      'Content-Type': 'application/json',
      'Authorization': MED_CLASSIFIER_SERVER_AUTH
    }

    response = requests.request("POST", url, headers=headers, data=payload, timeout=60)

    if response.status_code == 200:
      out = response.json()
      return out.get('result')
      
    return None
    
  except Exception as e:
    logger.error("Exception in invoking search framework - %s", str(e), exc_info=True)
    return None