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

def check_value(list_of_dicts, key):
    for dict_ in list_of_dicts:
        if dict_.get(key) == 1:
            return 1
    return 0

def get_lowest_rank(list_of_dicts):
    ranks = []
    for dict_ in list_of_dicts:
        if dict_['ddx_match'] == 1:
            ranks.append(dict_['rank'])
    try:
        return min(ranks) if ranks else None
    except:
        return None
    
def generate_analytics_for_case_study(
    jivi_system_differential_diagnosis
):
    if not jivi_system_differential_diagnosis:
        return None
    
    sorted_confidence_diagnosis = sorted(jivi_system_differential_diagnosis, key=lambda k: float(k['Confidence']), reverse=True)
    out = []
    ddx_match = None
    ddx_rank = 0
    match_classes = ['Match','Strong Match']
    non_match_classes = ['Not Match', 'Weak Match']
    for diagnosis in sorted_confidence_diagnosis:
        
        if diagnosis.get('Diagnosis-Match-Value') in match_classes:
            ddx_match = 1
            logger.info("Diagnosis matched with actual diagnosis")

        if diagnosis.get('Diagnosis-Match-Value') in non_match_classes:
            ddx_match = 0
        
        diagnosis_match = {}
        diagnosis_match['ddx_match'] = ddx_match
        ddx_rank+=1
        diagnosis_match['rank'] = ddx_rank
        out.append(diagnosis_match)

    
    if len(out) > 0:
        ddx_match_value = check_value(out,"ddx_match")
        lower_rank = get_lowest_rank(out)
    
        return {
            "ddx_match_value":ddx_match_value,
            "rank":lower_rank
        }
        
    
        