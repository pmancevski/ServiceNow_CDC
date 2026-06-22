import json
import requests
import os

def lambda_handler(event, context):
    url = f"{os.environ['SERVICENOW_URL']}/api/now/table/incident"
    auth = (os.environ['SERVICENOW_USER'], os.environ['SERVICENOW_PASSWORD'])
    
    response = requests.get(url, auth=auth, params={'sysparm_limit': 100})
    response.raise_for_status()
    
    result = response.json().get('result', [])
    
    # Filter only needed fields
    incidents = []
    for inc in result:
        incidents.append({
            'number': inc.get('number'),
            'short_description': inc.get('short_description'),
            'state': inc.get('state'),
            'category': inc.get('category'),
            'urgency': inc.get('urgency'),
            'opened_at': inc.get('opened_at'),
            'sys_id': inc.get('sys_id')
        })
    
    return {
        'statusCode': 200,
        'headers': {'Access-Control-Allow-Origin': '*'},
        'body': json.dumps(incidents)
    }
