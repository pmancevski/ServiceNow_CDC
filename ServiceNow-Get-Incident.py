import json
import requests
import os

def lambda_handler(event, context):

    print(f"Event: {event}")
    print(f"Path parameters: {event.get('pathParameters')}")

    try:
        ticket_number = event.get('pathParameters', {}).get('id')
        
        if not ticket_number:
            return {
                'statusCode': 400,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': 'Incident ID is required'})
            }
        
        url = f"{os.environ['SERVICENOW_URL']}/api/now/table/incident?sysparm_query=number={ticket_number}"
        auth = (os.environ['SERVICENOW_USER'], os.environ['SERVICENOW_PASSWORD'])
        
        response = requests.get(url, auth=auth, headers={'Accept': 'application/json'})
        response.raise_for_status()
        
        print("MY TEST MY TEST 01")

        result = response.json().get('result', {})[0]

        print("MY TEST MY TEST 02")
        print(f"Full result: {result}")
        
        return {
            'statusCode': 200,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({
                'number': result.get('number'),
                'short_description': result.get('short_description'),
                'description': result.get('description'),
                'state': result.get('state'),
                'category': result.get('category'),
                'urgency': result.get('urgency'),
                'opened_at': result.get('opened_at'),
                'sys_id': result.get('sys_id')
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)})
        }
