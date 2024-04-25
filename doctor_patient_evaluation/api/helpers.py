import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

from doctor_patient_evaluation.api.api import invoke_user_conversation
import time

def invoke_custom_user_conversation(
  session_id,
  user_input,
  env
):
  try:
    response = invoke_user_conversation(
        session_id=session_id,
        env=env,
        user_input=user_input,
        user_id='patient-agent'
    )
    return response['result']['response'][0]['header'],response['result']['response'][0]['header_code']
  except Exception as e:
    logger.error(f"Error in user conversation - {e}", exc_info=True)
    logger.info(f"Error in user conversation invoking again")
    response = invoke_user_conversation(
        session_id=session_id,
        env=env,
        user_input=user_input,
        user_id='patient-agent'
    )
    return response['result']['response'][0]['header'],response['result']['response'][0]['header_code']

