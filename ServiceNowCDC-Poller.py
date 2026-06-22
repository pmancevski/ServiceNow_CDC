import json
import boto3
import requests
from datetime import datetime, timedelta
import os
from kafka import KafkaProducer

dynamodb = boto3.client('dynamodb')
TABLE_NAME = os.environ['DYNAMODB_TABLE']

def lambda_handler(event, context):
    try:
        # Read last poll time from DynamoDB
        response = dynamodb.get_item(
            TableName=TABLE_NAME,
            Key={'id': {'S': 'lastpoll'}}
        )

        if 'Item' in response:
            last_poll = response['Item']['timestamp']['S']
        else:
            last_poll = (datetime.now() - timedelta(hours=24)).isoformat()
        
        print(f"Last poll time: {last_poll}")

        # ServiceNow API setup
        url = f"{os.environ['SERVICENOW_URL']}/api/now/table/sys_audit"
        auth = (os.environ['SERVICENOW_USER'], os.environ['SERVICENOW_PASSWORD'])
        query = f'sys_created_on>{last_poll}'
        limit = 1000
        offset = 0
        all_records = []

        # Step 1: Get total count
        count_res = requests.get(url, auth=auth, params={'sysparm_query': query, 'sysparm_count': 'true'})
        total = int(count_res.headers.get('X-Total-Count', 0))
        print(f"Total records to fetch: {total}")

        # Step 2: Paginate until all records are fetched
        while offset < total:
            params = {
                'sysparm_query': query,
                'sysparm_limit': str(limit),
                'sysparm_offset': str(offset)
            }
            res = requests.get(url, auth=auth, params=params)
            res.raise_for_status()
            records = res.json().get('result', [])
            all_records.extend(records)
            print(f"Fetched {len(records)} records (offset {offset})")
            offset += limit

        print(f"Total fetched: {len(all_records)} records")

        # Publish to Kafka
        producer = KafkaProducer(
            bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],
            security_protocol='SASL_SSL',
            sasl_mechanism='PLAIN',
            sasl_plain_username=os.environ['KAFKA_API_KEY'],
            sasl_plain_password=os.environ['KAFKA_API_SECRET'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        for record in all_records:
            change_event = {
                'created_on': record.get('sys_created_on'),
                'table_name': record.get('tablename'),
                'record_id': record.get('documentkey'),
                'field_name': record.get('fieldname'),
                'old_value': record.get('oldvalue'),
                'new_value': record.get('newvalue'),
                'changed_by': record.get('user'),
                'audit_id': record.get('sys_id')
            }
            producer.send(os.environ['KAFKA_TOPIC'], value=change_event)
        
        producer.flush()
        print(f"Published {len(all_records)} records to Kafka")

        # Update last poll time
        dynamodb.put_item(
            TableName=TABLE_NAME,
            Item={
                'id': {'S': 'lastpoll'},
                'timestamp': {'S': datetime.now().isoformat()}
            }
        )

        return {
            'statusCode': 200,
            'body': json.dumps({
                'last_poll': last_poll,
                'records_found': len(all_records),
                'published_to_kafka': len(all_records)
            })
        }

    except Exception as e:
        print(f"Error: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
