import json
import requests
import os
import uuid
from kafka import KafkaProducer

def lambda_handler(event, context):
    try:
        print(f"Event: {event}")
        # body = json.loads(event.get('body', '{}'))
        
        url = f"{os.environ['SERVICENOW_URL']}/api/now/table/incident"
        auth = (os.environ['SERVICENOW_USER'], os.environ['SERVICENOW_PASSWORD'])

        payload = {
            'short_description': event.get('short_description'),
            'description': event.get('description'),
            'category': event.get('category'),
            'urgency': event.get('urgency'),
            'caller_id': os.environ.get('DEFAULT_CALLER')
        }
        
        # payload = {
        #     'short_description': body.get('short_description'),
        #     'description': body.get('description'),
        #     'category': body.get('category'),
        #     'urgency': body.get('urgency'),
        #     'caller_id': os.environ.get('DEFAULT_CALLER')
        # }

        print(f"Payload: {payload}")
        
        response = requests.post(url, auth=auth, json=payload, headers={'Content-Type': 'application/json'})
        response.raise_for_status()

        result = response.json().get('result', {})

        # Publish to Kafka
        producer = KafkaProducer(
            bootstrap_servers=os.environ['KAFKA_BOOTSTRAP_SERVERS'],
            security_protocol='SASL_SSL',
            sasl_mechanism='PLAIN',
            sasl_plain_username=os.environ['KAFKA_API_KEY'],
            sasl_plain_password=os.environ['KAFKA_API_SECRET'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        kafka_message = {
            'created_on': result.get('sys_created_on'),
            'table_name': result.get('sys_class_name'),
            'record_id': result.get('sys_id'),
            'field_name': 'creation',
            'old_value': '',
            'new_value': '1',
            'changed_by': os.environ.get('DEFAULT_CALLER'),
            'audit_id': str(uuid.uuid4()).replace('-', ''),
            'ticket_number': result.get('number')
        }

        producer.send(os.environ['KAFKA_TOPIC'], value=kafka_message)
        producer.flush()
        producer.close()
        
        return {
            'statusCode': 200,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({
                'success': True,
                'sys_id': result.get('sys_id'),
                'number': result.get('number')
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)})
        }
