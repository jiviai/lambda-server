import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

from datetime import datetime
import json
import os
import concurrent.futures
import uuid

from search_content.api.api import invoke_content_creation_agent, invoke_language_translation_framework, invoke_bq_post_service
from search_content.utils import validate_dynamo_save_payload
from search_content.db import DynamoDBClient
from search_content.opensearch import OpensearchLoader

dynamo_client = DynamoDBClient(
    table_name=f"query_search_result_{os.environ.get('CURRENT_STAGE','uat')}",
    region_name='ap-south-1'
)
opensearch_loader = OpensearchLoader(
    index_name=os.environ.get("CONTENT_CACHE_EMBEDDING_INDEX","jivi_search_content_index")
)

def lambda_handler(
    event,
    context
):        
    for record in event['Records']:
        logger.info(f"Row for dynamo: {record}")
        
        if record['eventName'] == 'INSERT':
            new_image = record.get('dynamodb').get('NewImage')

            if new_image is not None:
                query_id = new_image.get('query_id').get('S')
                search_entity_keys = json.loads(new_image.get('search_entity_keys').get('S'))
                entity = new_image.get('entity').get('S')
                query = new_image.get('query').get('S')
                query_original = new_image.get('query_original').get('S')
                related_queries = json.loads(new_image.get('related_queries').get('S'))
                embedding = json.loads(new_image.get('query_embedding').get('S'))
                chat_response = new_image.get('chat_response').get('S')
                language = new_image.get('language').get('S')
                user_id = new_image.get('user_id').get('S')
                created_at = new_image.get('created_at').get('S')

                logger.info("Query ID: %s search_entity_keys: %s entity: %s query: %s", query_id, search_entity_keys, entity, query)
                                
                api_payloads = [
                        {
                            'agent_name': os.environ.get('CONTENT_CREATION_AGENT_NAME','user_query_topic_v1'),
                            'query': query,
                            'entity': entity,
                            'topic': key
                        } for key in search_entity_keys
                    ]
                    
                with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                    logger.info("Invoking parallel api calls")
                    
                    # Submit all the tasks
                    future_to_params = {
                        executor.submit(
                            invoke_content_creation_agent,
                            params['agent_name'],
                            params['query'],
                            params['entity'],
                            params['topic']
                        ): params for params in api_payloads
                    }
                    
                    for future in concurrent.futures.as_completed(future_to_params):
                        params = future_to_params[future]
                        try:
                            content = future.result()
                            _save = {
                                'query_id': query_id,
                                'content_id': str(uuid.uuid4()),
                                'topic': params['topic'],
                                'entity': params['entity'],
                                'query': params['query'],
                                'content': content,
                                'created_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
                            }
                            _save_opensearch = {
                                "query_original":query_original,
                                "query":query,
                                "embedding":embedding,
                                "entity":entity,
                                "search_entity_keys":json.dumps(search_entity_keys),
                                "related_queries":json.dumps(related_queries),
                                "chat_response":chat_response,
                                "content": content,
                                "topic": params['topic'],
                                "language":language
                            }
                            opensearch_loader.save_obj(obj=_save_opensearch)
                            dynamo_client.add_item(item=validate_dynamo_save_payload(obj=_save))
                        except Exception as exc:
                            logger.error(f"API call with params {params} generated an exception: {exc}")
                
                bq_payload = {
                    "user_id":user_id,
                    "query": query,
                    "query_id": query_id,
                    "search_entity_keys": new_image.get('search_entity_keys').get('S'),
                    "entity": new_image.get('entity').get('S'),
                    "query_original": new_image.get('query_original').get('S'),
                    "related_queries":  new_image.get('related_queries').get('S'),
                    "embedding": new_image.get('query_embedding').get('S'),
                    "chat_response": new_image.get('chat_response').get('S'),
                    "language": new_image.get('language').get('S'),
                    "created_at": created_at
                }
                
                bq_service_response = invoke_bq_post_service(
                    payload_dict=bq_payload,
                    table_name=os.environ.get("BQ_TABLE_NAME","query_search_prod_v2"),
                    db_name=os.environ.get('BQ_DB_NAME','jivihealth')
                )
                
                logger.info(f"Response from bq service response - {bq_service_response}")
                  
            else:
                logger.error("No new image found in the record or the image is broken")
            