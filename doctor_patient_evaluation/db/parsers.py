import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

import pandas as pd

from doctor_patient_evaluation.db.helpers import get_chat_history

def parse_conversation_history_response(
    conversation_history_response
):
    try:
        if not conversation_history_response:
            return None
        
        #messages = conversation_history_response.get('data').get('messages')
        
        parsed_messages = []
        for message in conversation_history_response:
            out = {}
            
            if message.get('type') == 'human':
                out['Agent'] = message.get('type')
                #out['created_at'] = message.get('created_at')
                out['Content'] = message.get('data').get('content')
                out['Response-Human'] = None
                out['Response-QuestionAgent'] = None
                out['Response-DifferentialDiagnosis'] = None
                #out['Response'] = None
                out['Response-Moderator'] = None
                parsed_messages.append(out)
                
            if message.get('type') == 'ai':
                out['Agent'] = message.get('type')
                #out['created_at'] = message.get('created_at')
                out['Content'] = message.get('data').get('content')
                out['Response-Human'] = None
                out['Response-QuestionAgent'] = None
                out['Response-DifferentialDiagnosis'] = None
                out['Response-IncrementalSummary'] = None
                #out['Response'] = None
                #out['moderator_response'] = None
                ai_messages = message.get('data').get('additional_kwargs').get('agent_output')
                for ai_message in ai_messages:
                    if ai_message.get('type') == 'human':
                        out['Response-Human'] = ai_message.get('data').get('content')
                    if ai_message.get('type') == 'question agent':
                        out['Response-QuestionAgent'] = ai_message.get('data').get('content')
                    if ai_message.get('type') == 'differential diagnosis agent':
                        ddx_out = ai_message.get('data').get('content')
                        if ddx_out:
                            ddx_out = ddx_out.replace("\n","\n\n\n")
                        out['Response-DifferentialDiagnosis'] = ddx_out
                        #out['Response'] = ai_message.get('data').get('content')
                    if ai_message.get('type') == 'moderator':
                        out['Response-Moderator'] = ai_message.get('data').get('content')
                    if ai_message.get('type') == 'incremental_summary':
                        out['Response-IncrementalSummary'] = ai_message.get('data').get('content')
                parsed_messages.append(out)
                
                
        #output_df = pd.DataFrame(parsed_messages)
        #reversed_output_df = output_df.iloc[::-1]
        # df = pd.DataFrame(parsed_messages)
        # df.to_csv("./test_df.csv")
        return parsed_messages
    except Exception as e:
        logger.error(f"Error in parse_conversation_history_response - {e}", exc_info=True)
        return None