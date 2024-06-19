import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

import json
import requests
import pandas as pd
import time
import re
from requests.exceptions import Timeout

from doctor_patient_evaluation.api.config import CONFIG
from doctor_patient_evaluation.api.utils import merge_dicts_to_text
from doctor_patient_evaluation.llm.llm import DdxMatchAgent

ddx_match_agent = DdxMatchAgent()

def format_confirmation(header,questions):
  res = f"{header}\n"
  for item in questions:
    res+=f"{item['code']}\n"
    
  return res

def parse_llm_response(llm_content):
  try:
    partial_match = re.search(r'(Match|Strong Match|Weak Match|Not Match)', llm_content)
    partial = partial_match.group(1) if partial_match else None

    # Extract Reason
    #reason_match = re.search(r'Reason:\n+(.*)', llm_content)
    reason = llm_content.replace(partial, '') if partial else None
    #reason = reason_match.group(1).strip() if reason_match else None

    output = {
      "match_value":partial,
      "match_reason":reason
    }
    logger.info("Match Dictionary Parsed: {}".format(output))
    return output
  except Exception as e:
    logger.error(f"Error in parse_llm_response - {e}", exc_info=True)
    return {}
  
def invoke_user_session_summary(
    session_id,
    env
):
    endpoint = CONFIG.get(env).get('endpoint')
    auth = CONFIG.get(env).get('auth')
    
    url = f"https://api-server.jivihealth.org{endpoint}/v1/user_session_summary?cache=true&force_cache=false"
    logger.info(f"endpoint from config for session summary - {url}")

    payload = json.dumps({
        "user_id": "patient-agent",
        "session_id": session_id,
        "user_input": "",
        "model_name": "gpt-3.5-turbo",
        "enable_history": False,
        "context": "",
        "system_propmt": ""
    })
    headers = {
      'accept': 'application/json',
      'Content-Type': 'application/json',
      'Authorization': auth
    }
    
    response = requests.request("POST", url, headers=headers, data=payload, timeout=60)
    logger.info(f"Response from session summary - {response.text} with status - {response.status_code}")
    
    if response.status_code == 200:
      out = response.json()
      res = out['result']['summary'] + "\n" + "\n" + "Clinical Notes:\n" + merge_dicts_to_text(out['result']['clinical_notes']) + "\n" + "Symptoms:\n" + merge_dicts_to_text(out['result']['symptoms'])
      return res
    return ""

def invoke_user_differential_diagnosis(
    session_id,
    env,
    actual_diagnosis
):
  try:
    endpoint = CONFIG.get(env).get('endpoint')
    auth = CONFIG.get(env).get('auth')

    url = f"https://api-server.jivihealth.org{endpoint}/v1/user_differential_diagnosis?cache=true&force_cache=false"
    logger.info(f"endpoint from config for diagnosis - {url}")

    payload = json.dumps({
        "user_id": "patient-agent",
        "session_id": session_id,
        "user_input": "",
        "model_name": "gpt-3.5-turbo",
        "enable_history": False,
        "context": "",
        "system_propmt": ""
    })
    headers = {
      'accept': 'application/json',
      'Content-Type': 'application/json',
      'Authorization': auth
    }

    response = requests.request("POST", url, headers=headers, data=payload, timeout=150)
    logger.info(f"Response from differential diagnosis - {response.text} with status - {response.status_code}")

    ddx = response.json()['result']['diagnosis']
    res = []
    for obj in ddx:
      match_res = ddx_match_agent.run(
        actual_diagnosis=actual_diagnosis,
        differential_diagnosis_result=obj['name']
      )
      match_dict = parse_llm_response(llm_content=match_res)
      logger.info(f"DDX matching result: {match_res}")
      out = {
        "Name":obj['name'],
        "Reason":obj['reason'],
        "Confidence":obj['actual_conf'],
        "Diagnosis-Match-Value":match_dict.get('match_value',None),
        "Diagnosis-Match-Reason":match_dict.get('match_reason',None)
      }
      res.append(out)
    return res
  except Exception as e:
    logger.error(f"Error in differential diagnosis - {e}", exc_info=True)
    return None

def invoke_user_conversation(
    session_id,
    env,
    user_input,
    code='cache',
    user_id='patient-agent',
    max_retries=1,
):
  retries = 0
  while retries < max_retries:
    try:
      start = time.time()
      endpoint = CONFIG.get(env).get('endpoint')
      auth = CONFIG.get(env).get('auth')

      url = f"https://api-server.jivihealth.org{endpoint}/v1/user_conv_qa_search"
      logger.info(f"endpoint from config for user conversation - {url}")

      payload = json.dumps({
        "symptoms": [
          {
            "header": None,
            "header_code": code,
            "code": [
              user_input
            ],
            "symptom_code": None
          }
        ],
        "user_id": user_id,
        "session_id": session_id,
        "meta": {},
        "model": "gpt-3.5-turbo",
        "doc_uri": [],
        "max_questions": 30,
        "min_questions": 5
      })
      logger.info(f"Request from user conversation - {payload}")
      headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': auth
      }
      logger.info(f"Request from user conversation - {payload}")
      response = requests.request("POST", url, headers=headers, data=payload, timeout=60)
      end = time.time()
      logger.info(f"Response from user conversation - {response.text} with status - {response.status_code}")
      logger.info(f"Response from user conversation time taken: {round((end-start),2)}")
      out = response.json()
      if out['result']['response'][0]['header_code'] == 'confirmation':
        payload = json.dumps({
          "symptoms": [
            {
              "header": None,
              "header_code": "confirmation",
              "code": [
                user_input
              ],
              "symptom_code": None
            }
          ],
          "user_id": user_id,
          "session_id": session_id,
          "meta": {},
          "model": "gpt-3.5-turbo",
          "doc_uri": [],
          "max_questions": 30,
          "min_questions": 5
        })
        confirm_response = requests.request("POST", url, headers=headers, data=payload, timeout=60)
        logger.info("Request Confirmation Invoked For Async Summary and Diagnosis")
        return format_confirmation(out['result']['response'][0]['header'],out['result']['response'][0]['questions']),out['result']['response'][0]['header_code']
      return out['result']['response'][0]['header'],out['result']['response'][0]['header_code']
    except Timeout:
      logger.error("Error in invocation of user conversation")
      retries+=1
  logger.info("Retries Exhausted, case flagged")
  return None, None