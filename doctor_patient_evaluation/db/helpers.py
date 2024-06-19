import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

import pandas as pd
from datetime import datetime
import uuid
import os

from boto3.dynamodb.conditions import Key

from doctor_patient_evaluation.db.dynamo import DynamoDBClient

def get_chat_history(
    user_id,
    session_id,
    env
):
    """
    Get chat history for a user.

    Parameters
    ----------
    user_id : str
        User ID.
    session_id : str
        Session ID.

    Returns
    -------
    list
        List of chat history items.
    """
    try:
        dynamo_db = DynamoDBClient(
                table_name=f'user_chat_history_{env}',
                region_name='ap-south-1'
            )
        
        if env in ['experiment']:
            dynamo_db = DynamoDBClient(
                table_name=f'user_history_search',
                region_name='ap-south-1'
            )

        output = []
        output = dynamo_db.query(
            indexName='user_id-session_id-index',
            query={
                "user_id":user_id,
                "session_id":session_id,
            }
        )
        # logger.info(f"Fetched chat history for user and session id- {user_id} {session_id}")
        sorted_output = sorted(output, key=lambda k: k['created_at'])
        return sorted_output
    except Exception as e:
        logger.error("some Exception occured main function dynamo- {}".format(str(e)), exc_info=True)
        return None
    
def upload(
    file,
    new_case_study_text,
    diagnosis,
    env,
    mode,
    ):
        dynamo_db = DynamoDBClient(
            table_name='system_evaluation',
            region_name='ap-south-1'
        )
        
        if mode == 'bulk':
            data = pd.read_excel(file)
            logger.info("Data read from excel with number of rows: %s", data.shape[0])
            
            question_set_id = f'bulk-{str(datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S"))}-question-set-{str(data.shape[0])}'
            logger.info("excel file name: %s", question_set_id)
            
            count = 1
            for row in data.to_dict('records'):
                updated_row = row.copy()
                updated_row['question_set_id'] = question_set_id
                updated_row['question_id'] = str(uuid.uuid4())
                updated_row['question_id_url'] = f'Case-{count}'
                updated_row['created_at'] = str(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))
                updated_row['env'] = env
                _save_evaluation_result = dynamo_db.add_item(
                    item=updated_row
                )
                count+=1
                logger.info("Questions saved to ddb")
            #gr.Info(message="Bulk Evaluation Started Successfully")
            return question_set_id
        
        if mode == 'single':
            _save = {}
            question_set_id = f'single-{str(datetime.utcnow().strftime("%Y-%m-%d-%H-%M-%S"))}-question-set-1'
            logger.info("question set id name: %s", question_set_id)
            
            _save['case_study'] = new_case_study_text
            _save['diagnosis'] = diagnosis
            _save['question_set_id'] = question_set_id
            _save['question_id'] = str(uuid.uuid4())
            _save['question_id_url'] = 'Case-1'
            _save['created_at'] = str(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f"))
            _save['env'] = env
            _save_evaluation_result = dynamo_db.add_item(
                    item=_save
                )
            logger.info("Question saved to ddb")
            #gr.Info(message="Single Evaluation Started Successfully")
            return question_set_id


def get_all_question_sets(table_name):
    dynamo_db = DynamoDBClient(
        table_name=table_name,
        region_name='ap-south-1'
    )
    res = dynamo_db.scan_table()    
    return list(set(res))


def get_all_questions(partition_key_value):
    dynamo_db = DynamoDBClient(
        table_name='system_evaluation',
        region_name='ap-south-1'
    )
    res = dynamo_db.get_items_by_partition_key(
        partition_key_attribute='question_set_id', 
        partition_key_value=partition_key_value
    )
    
    logger.info(f"All Questions Fetched")
    sorted_data = sorted(res, key=lambda x: int(x["question_id_url"].split("-")[-1]))

    out = []
    for result in sorted_data:
        out.append(result['question_id_url'])
    
    logger.info(f"sorted case studies: {out}")

    new_out = list(set(out))
    return new_out
    #gr.Dropdown(choices=new_out, label='Select Your Case ID')

def get_evaluation_result_from_db(
    question_set_id,
    question_id_url
):
    dynamo_db = DynamoDBClient(
        table_name='system_evaluation_result',
        region_name='ap-south-1'
    )
    
    result = None
    payload = {'KeyConditionExpression': Key('question_set_id').eq(question_set_id)}
    output = dynamo_db.read_data_from_ddb(payload)
    if len(output) != 0:
        for row in output:
            if row['question_id_url'] == question_id_url:
                result = row
    return result
def get_evaluation_question_from_db(
    question_set_id,
    question_id_url
):
    dynamo_db = DynamoDBClient(
        table_name='system_evaluation',
        region_name='ap-south-1'
    )
    
    result = None
    payload = {'KeyConditionExpression': Key('question_set_id').eq(question_set_id)}
    output = dynamo_db.read_data_from_ddb(payload)
    if len(output) != 0:
        for row in output:
            if row['question_id_url'] == question_id_url:
                result = row
    return result

def save_evaluation_result(row,table_name):
    dynamo_db = DynamoDBClient(
            table_name=table_name,
            region_name='ap-south-1'
        )
    dynamo_db.add_item(row)
    logger.info("Evaluation Result Saved")
    
