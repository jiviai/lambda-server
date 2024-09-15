import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

from datetime import datetime
import json
import os

from related_queries.api.api import invoke_related_queries_agent, invoke_embedding, invoke_search_api
from related_queries.opensearch import OpensearchLoader

opensearch_loader = OpensearchLoader(
    index_name=os.environ.get("RELATED_SEARCHES_INDEX_NAME","jivi_related_searches_index")
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
                query = new_image.get('query').get('S')
                language = new_image.get('language').get('S')
                entity = new_image.get('entity').get('S')
                
                logger.info("Query: %s Entity: %s", query, entity)
                
                if entity not in ["GENERAL", "NON_MEDICAL"]:
                #Invoke Agent API
                agent_result = invoke_related_queries_agent(
                    agent_name="autocomplete_related_queries_v1",
                    query=query
                )
                
                if agent_result:
                    for result in agent_result:
                        _save_opensearch = {}
                        embedding_response = invoke_embedding(
                            text=result,
                            model_name=os.environ.get('DEFAULT_EMBEDDING_MODEL','sentence-transformers/all-MiniLM-L12-v2')
                        )
                        # _med_classifier_response = invoke_search_api(
                        #     query=result,
                        #     language=language
                        # )
                        # logger.info("med classifier invoked response - %s", _med_classifier_response)
                        if embedding_response:
                            embedding = embedding_response.get('result').get('embedding')[0]
                            _save_opensearch['query'] = result
                            _save_opensearch['embedding'] = embedding
                            _save_opensearch['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

                            opensearch_loader.save_obj(
                                obj=_save_opensearch
                            )
                            logger.info("Saved to Opensearch - %s", _save_opensearch)
                        
                        