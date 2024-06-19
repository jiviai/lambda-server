import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

import pandas as pd
import uuid
import json
import time

from doctor_patient_evaluation.llm.llm import PatientAgent, SummaryAgent, DdxAgent, MedHistoryTakerAgent
from doctor_patient_evaluation.api.api import invoke_user_differential_diagnosis, invoke_user_session_summary, invoke_user_conversation
#from doctor_patient_evaluation.api.helpers import invoke_custom_user_conversation
from doctor_patient_evaluation.db.helpers import get_chat_history
from doctor_patient_evaluation.db.parsers import parse_conversation_history_response

patient_agent = PatientAgent()
summary_agent = SummaryAgent()
ddx_agent = DdxAgent()
med_history_taker_agent = MedHistoryTakerAgent()

def evaluate_new_case(
    new_case_study_text,
    env,
    diagnosis
    #session_id,
    #case_study,
    #actual_differential_diagnosis
):
    session_id = str(uuid.uuid4())
    try:
        failed = False
        session_id = str(uuid.uuid4())
        logger.info(f"new session id created: {session_id}")
        
        symptom = str(patient_agent.run("what is your primary complain?",new_case_study_text))
        conv = f"Patient:{symptom}"
        code = "none"
        list_out = [{"Type":"Patient", "Conversation":symptom}]
        
        while code!='confirmation':
            question,code = invoke_user_conversation(
                session_id=session_id,
                env=env,
                user_input=symptom,
                code=code
            )
            if (question is None and code is None):
                failed = True
                logger.info(f"case study skipped because of user conversation not responding - {failed}")
                break
                
            logger.info(f"Doctor:{question}")
            conv +="\n "+f"Doctor:{question}"
            out = {}
            out['Type'] = 'Doctor'
            out['Conversation'] = question
            list_out.append(out)
            symptom = str(patient_agent.run(
                question=question,
                case_study=new_case_study_text
            ))
            out = {}
            out['Type'] = 'Patient'
            out['Conversation'] = symptom
            list_out.append(out)
            conv +="\n "+f"Patient:{symptom}"
            logger.info(f"Patient:{symptom}")
        
        #Med History Enabler
        # med_history_result = fetch_med_history(
        #     case_study=new_case_study_text
        # )
        
        # doc_patient_conv = ""
        # symptom = str(patient_agent.run("what is your primary complain?", new_case_study_text))
        # list_out = [{"Type":"Patient", "Conversation":symptom}]

        # doc_patient_conv = f"Assistant: what is your primary complain? \n Patient: {symptom}"
        # questions = json.loads(med_history_taker_agent.run(symptom))['questions']
        # for question in questions:
        #     out = {}
        #     out['Type'] = 'Med History Taker'
        #     out['Conversation'] = question
        #     list_out.append(out)
        #     logger.info(f"Med History Taker:{question}")
        #     symptom = str(patient_agent.run(question,new_case_study_text))
        #     out = {}
        #     out['Type'] = 'Patient'
        #     out['Conversation'] = symptom
        #     list_out.append(out)
        #     logger.info(f"Patient:{symptom}")
        #     doc_patient_conv += f"\n Assistant: {question} \n Patient: {symptom}"
        
        # conv = doc_patient_conv
        
        # summary_history = summary_agent.run(
        #     doc_patent_conv=conv
        # )
        # symptom = summary_history
        # code = "none"
        # list_out.append({"Type":"Med History", "Conversation":symptom})
        # logger.info(f"Med History:{symptom}")

        # while code!='summary':
        #     question, code = invoke_custom_user_conversation(
        #         session_id=session_id,
        #         user_input=symptom,
        #         env=env
        #     )
        #     out = {}
        #     out['Type'] = 'Doctor'
        #     out['Conversation'] = question
        #     list_out.append(out)
        #     logger.info(f"Doctor:{question}")
        #     symptom = str(patient_agent.run(
        #         question=question,
        #         case_study=new_case_study_text
        #     ))
        #     out = {}
        #     out['Type'] = 'Patient'
        #     out['Conversation'] = symptom
        #     list_out.append(out)
        #     logger.info(f"Patient:{symptom}")
        logger.info("Waiting for 150 seconds to get the summary from the system...")
        time.sleep(100)
        
        jivi_system_summary = invoke_user_session_summary(
            session_id=session_id,
            env=env
        )
        
        logger.info("Waiting for 150 seconds to get the ddx from the system...")
        time.sleep(100)

        jivi_system_differential_diagnosis = invoke_user_differential_diagnosis(
            session_id=session_id,
            env=env,
            actual_diagnosis=diagnosis
        )
        
        ddx_agent_response = get_chat_history(
            user_id='patient-agent',
            session_id=session_id,
            env=env
        )
        
        ddx_df = parse_conversation_history_response(
            conversation_history_response=ddx_agent_response
        )
        
        summary_ddx_response = ddx_agent.run(
            patient_summary=jivi_system_summary
        )
        
        return session_id, failed, list_out, jivi_system_summary, jivi_system_differential_diagnosis, ddx_df, summary_ddx_response
    
    except Exception as e:
        logger.error(f"Error in evaluation - {e}", exc_info=True)
        return session_id, [{}],[{}],"",[{}],[{}],""
