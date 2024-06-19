import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

from doctor_patient_evaluation.api.api import invoke_user_conversation
import time

# def invoke_custom_user_conversation(
#   session_id,
#   user_input,
#   env,
#   max_retries=1
# ):
#     retries = 0
#     while retries < max_retries:
#       response = invoke_user_conversation(
#         session_id=session_id,
#         env=env,
#         user_input=user_input,
#         user_id='patient-agent'
#       )
#       return response['result']['response'][0]['header'],response['result']['response'][0]['header_code']
#     logger.error(f"Error in user conversation - {e}", exc_info=True)
#     logger.info(f"Invoking again due to exception")
#     response = invoke_user_conversation(
#         session_id=session_id,
#         env=env,
#         user_input=user_input,
#         user_id='patient-agent'
#     )
#     return response['result']['response'][0]['header'],response['result']['response'][0]['header_code']

