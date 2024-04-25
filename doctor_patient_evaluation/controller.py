import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

import json
import base64
from datetime import datetime
import requests

from doctor_patient_evaluation.service.evaluator import evaluate_new_case
from doctor_patient_evaluation.db.helpers import save_evaluation_result
from doctor_patient_evaluation.service.utils import convert_float_to_decimal
from doctor_patient_evaluation.service.analytics import generate_analytics_for_case_study

def decode_and_deserialize_payload(kinesis_payload):
    """
    Decodes a bytes object that contains a serialized JSON string,
    and then deserializes it back to the original Python dictionary.
    
    :param encoded_serialized_payload: The payload as a bytes object, 
                                       containing a serialized JSON string.
    :return: The original dictionary.
    """
    payload = base64.b64decode(kinesis_payload).decode('utf-8')
    payload_dict = json.loads(payload)
    return payload_dict

def lambda_handler(
    event,
    context
):
    logger.info(f"Payload received for lambda processing: {event}")
        
    for record in event['Records']:
        result = {}
        #kinesis_payload = record.get('kinesis').get('data')
        # parsed_payload = decode_and_deserialize_payload(
        #     kinesis_payload=kinesis_payload
        # )
        logger.info(f"Row for dynamo: {result}")
        
        if record['eventName'] == 'INSERT':
            new_image = record.get('dynamodb').get('NewImage')

            if new_image is not None:
                case_study = new_image.get('case_study').get('S')
                env = new_image.get('env').get('S')
                question_set_id = new_image.get('question_set_id').get('S')
                question_id = new_image.get('question_id').get('S')
                question_id_url = new_image.get('question_id_url').get('S')
                diagnosis = new_image.get('diagnosis').get('S')

                conv, jivi_system_summary, jivi_system_differential_diagnosis, ddx_df, summary_ddx = evaluate_new_case(
                    new_case_study_text=case_study,
                    env=env,
                    diagnosis=diagnosis
                )
                
                analytics_result = generate_analytics_for_case_study(
                    jivi_system_differential_diagnosis=jivi_system_differential_diagnosis
                )
                #logger.info(f"Evaluation result - {evaluation_result}")
                
                result['question_set_id'] = question_set_id
                result['question_id'] = question_id
                result['question_id_url'] = question_id_url
                result['conv'] = conv
                result['jivi_system_summary'] = jivi_system_summary
                result['jivi_system_differential_diagnosis'] = jivi_system_differential_diagnosis
                result['ddx_df'] = ddx_df
                result['summary_ddx'] = summary_ddx
                result['created_at'] = str(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))
                result['actual_differential_diagnosis'] = diagnosis
                result['env'] = env
                result['case_study'] = case_study
                result['diagnosis_rank'] = analytics_result.get('rank')
                result['ddx_match_value'] = analytics_result.get('ddx_match_value')

                save_evaluation_result(convert_float_to_decimal(result))
                logger.info(f"evaluation result saved - {question_id} for {question_set_id}")                
            

    # return {
    #         "status": 200,
    #         "message": "success"
    #     }


    
    
    