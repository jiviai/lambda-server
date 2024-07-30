import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

import os
from opensearchpy import OpenSearch, RequestsHttpConnection, helpers

opensearch_url = os.environ.get('opensearch_url')
opensearch_username = os.environ.get('opensearch_username')
opensearch_password = os.environ.get('opensearch_password')

class OpensearchLoader:
    
    def __init__(self, index_name):
        self.index = index_name
        self.host = opensearch_url
        self.auth = (opensearch_username, opensearch_password)

        self.client = OpenSearch(
            hosts=[{'host': self.host, 'port': 443}],
            http_auth=self.auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
            pool_maxsize=20
        )
        logger.info("Opensearch Client Initialized")
    
    def save_obj(
        self,
        obj
    ):
        results = helpers.bulk(self.client, [obj], index=self.index, raise_on_error=True, refresh=True)
        logger.info("Content Response Saved to Opensearch")
        return results