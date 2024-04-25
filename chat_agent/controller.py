import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

from chat_agent.user.controller import invoke_user_token_generate
from chat_agent.chat.controller import invoke_chat, invoke_initiate_chat

import json
def lambda_handler(
    event,
    context
):
    logger.info(f"Incoming payload from : {event}")
    try:
        env="uat"

        #Get the body of the api request
        api_body = json.loads(event.get('body'))
        
        if api_body.get('user_type') == 'NEW':
            #Register the user and generate the user id and token
            generate_user_response = invoke_user_token_generate(
                env='staging'
            )
            
            #Retrieve token and user_id and generate chat
            token = generate_user_response.get('data').get('token')
            user_id = generate_user_response.get('data').get('user').get('id')
            user_type = generate_user_response.get('data').get('user').get('userType')
            
            #Invoke chat init for session initiation
            generate_chat_init_response = invoke_initiate_chat(
                token=token,
                user_type=user_type,
                env='staging'
            )
             
            session_id = generate_chat_init_response.get('data').get('sessionId')
            
            generate_chat_response = invoke_chat(
                session_id=session_id,
                message=api_body.get('message'),
                token=token,
                user_type=user_type,
                env='staging'
            )
            
            response_message = generate_chat_response.get('data').get('response')[0].get('header')
            header_code = generate_chat_response.get('data').get('response')[0].get('header_code')
            
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "data":{
                        "response":response_message,
                        "header_code":header_code,
                        "user_id":user_id,
                        "session_id":session_id,
                        "token":token,
                        "user_type":user_type
                    },
                    "message": "OK"
                })
            }
            
        if api_body.get('user_type') == 'EXISTING':
            
            #Retrieve token and user_id and generate chat
            token = api_body.get('data').get('token')
            user_id = api_body.get('data').get('user_id')
            user_type = api_body.get('data').get('user_type')
            session_id = api_body.get('data').get('session_id')
                        
            generate_chat_response = invoke_chat(
                session_id=session_id,
                message=api_body.get('message'),
                token=token,
                user_type=user_type,
                env='staging'
            )
            
            response_message = generate_chat_response.get('data').get('response')[0].get('header')
            header_code = generate_chat_response.get('data').get('response')[0].get('header_code')
            
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "data":{
                        "response":response_message,
                        "header_code":header_code,
                        "user_id":user_id,
                        "session_id":session_id,
                        "token":token,
                        "user_type":user_type
                    },
                    "message": "OK"
                })
            }
        #If api body does not meet criteria
        return {
                "statusCode": 400,
                "body": json.dumps({
                    "message":f"Error in processing event"
                })
            }
    except Exception as e:
        logger.error(f"Error in processing chat request - {e}", exc_info=True)
        return {
            "statusCode": 400,
            "body": json.dumps({
                "message":str(e)
            })
        }
        