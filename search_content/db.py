import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.info("Loaded " + __name__)

import traceback
import boto3
from botocore.exceptions import ClientError
from datetime import datetime


class DynamoDBClient:
    def __init__(self, table_name, region_name):
        self.table_name = table_name
        self.connect(table_name, region_name)


    def connect(self,table_name, region_name):
        try:
            logger.info("DynamoDB connection initialised")
            self.dynamodb = boto3.resource('dynamodb',region_name=region_name)
            self.table = self.dynamodb.Table(table_name)
            logger.info("DynamoDB connection Successfull")
        except ClientError as e:
            logger.error("some Exception occured dynamo connection failed- {}".format(str(e)))
            traceback.print_exc()


    def add_item(self, item):
        try:
            response = self.table.put_item(Item=item)
            logger.info("Item saved to Dynamodb")
            return response
        except ClientError as e:
            logger.error("some Exception occured - {}".format(str(e)))
            traceback.print_exc()


    def get_item(self, key):
        try:
            response = self.table.get_item(Key=key)
            return response.get('Item', None)
        except ClientError as e:
            logger.error("some Exception occured - {}".format(str(e)))
            traceback.print_exc()


    def delete_item(self, key):
        try:
            logger.info("deleting - {}".format(str(key)))
            response = self.table.delete_item(Key=key)
            return response
        except ClientError as e:
            logger.error("some Exception occured - {}".format(str(e)))
            traceback.print_exc()


    def query_items(self, query_params):
        try:
            response = self.table.query(**query_params)
            return response.get('Items', [])
        except ClientError as e:
            logger.error("some Exception occured - {}".format(str(e)))
            traceback.print_exc()

    def query(self,indexName,query):
        try:
            key_condition_expression = ' AND '.join([f'{key} = :{key}' for key in query.keys()])
            expression_attribute_values = {f':{key}': value for key, value in query.items()}

            # Perform the query
            response = self.table.query(
                IndexName=indexName,
                KeyConditionExpression=key_condition_expression,
                ExpressionAttributeValues=expression_attribute_values
            )

            # Get the list of items
            items = response['Items']
            return items
        except ClientError as e:
            logger.error("some Exception occured  - {}".format(str(e)))
            traceback.print_exc()
            return None


    def get_latest_item(self, user_id, session_id,type):
        try:
            response = self.table.query( # Replace with your GSI name
                KeyConditionExpression=boto3.dynamodb.conditions.Key('partition_key_name').eq(user_id) & 
                                       boto3.dynamodb.conditions.Key('sort_key_name').eq(session_id),
                ScanIndexForward=False,  # Sorts the results in descending order
                Limit=1  # Retrieves only the latest item
            )
            items = response.get('Items', [])
            return items[0] if items else None
        except ClientError as e:
            logger.error("some Exception occured - {}".format(str(e)))
            traceback.print_exc()


    def scan_table(self):
        try:
            response = self.table.scan()
            data = response['Items']
            # Handling potential pagination
            while 'LastEvaluatedKey' in response:
                response = self.table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                data.extend(response['Items'])

            return data
        except ClientError as e:
            logger.error("some Exception occured - {}".format(str(e)))
            traceback.print_exc()
            

    def purge_entries_after_msg_id(self, msg_id):
        try:
            logger.info("Purging entries after" + str(msg_id))
            # Query items using the secondary index
            items = self.query(
                indexName="msg_id-index",
                query={'msg_id': msg_id}
            )
            if items:
                logger.info("msg - {}".format(items))
                # Get the first item (assuming there is only one)
                item = items[0]
                user_id = item.get('user_id')
                session_id = item.get('session_id')
                created_at_str = item.get('created_at')
                created_at = datetime.strptime(created_at_str, '%Y-%m-%d %H:%M:%S.%f')
                logger.info("user_id - {} session_id - {}".format(user_id,session_id))
                # Query items that match the user_id, session_id, and created_at criteria
                response = self.query(
                        indexName="user_id-session_id-index",
                        query={'user_id': user_id,"session_id":session_id})
                # logger.info("chat history - {}".format(response))
                # Delete items with created_at greater than the creation time of the specified message
                for item in response:
                    item_created_at_str = item.get('created_at', None)
                    temp_msg_id = item.get('msg_id', None)
                    # logger.info("msg id to purge - {}".format(temp_msg_id))
                    if item_created_at_str:
                        item_created_at = datetime.strptime(item_created_at_str, '%Y-%m-%d %H:%M:%S.%f')
                        if item_created_at >= created_at:
                            # Delete item if created_at is greater than the creation time of the specified message
                            self.delete_item({'msg_id': temp_msg_id,'user_id':user_id})
                status = True
                logger.info("purge status - {}".format(status))
                return status  # Successful purge
            else:
                logger.error("Message ID not found")
                return False  # Unsuccessful purge
        except Exception as e:
            logger.error("some Exception occured main function dynamo- {}".format(str(e)))
            traceback.print_exc()