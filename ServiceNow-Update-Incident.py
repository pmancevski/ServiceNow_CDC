import json
import requests
import os

def lambda_handler(event, context):
    try:
        ticket_number = event.get('pathParameters', {}).get('id')
        body = json.loads(event.get('body', '{}'))
        
        # Query by ticket number
        url = f"{os.environ['SERVICENOW_URL']}/api/now/table/incident?sysparm_query=number={ticket_number}"
        auth = (os.environ['SERVICENOW_USER'], os.environ['SERVICENOW_PASSWORD'])
        
        # Get the incident first
        get_response = requests.get(url, auth=auth, headers={'Accept': 'application/json'})
        get_response.raise_for_status()
        result_list = get_response.json().get('result', [])
        
        if not result_list:
            return {
                'statusCode': 404,
                'headers': {'Access-Control-Allow-Origin': '*'},
                'body': json.dumps({'error': 'Incident not found'})
            }
        
        incident = result_list[0]
        incident_id = incident.get('sys_id')
        
        # Append new description to old description
        old_description = incident.get('description', '')
        new_description = body.get('description')
        
        if old_description and new_description:
            updated_description = f"{old_description}\n\n=============================\n\n{new_description}"
        else:
            updated_description = new_description or old_description
        
        # Build payload
        payload = {
            'description': updated_description,
            'state': body.get('state')
        }
        
        # If state is Resolved (6), add close_notes and close_code
        if body.get('state') == '6':
            payload['close_notes'] = body.get('close_notes') or 'Resolved via API'
            payload['close_code'] = body.get('close_code') or 'Solution provided'
        
        # Update the incident
        update_url = f"{os.environ['SERVICENOW_URL']}/api/now/table/incident/{incident_id}"
        response = requests.patch(update_url, auth=auth, json=payload, headers={'Content-Type': 'application/json'})
        response.raise_for_status()
        
        result = response.json().get('result', {})
        
        return {
            'statusCode': 200,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({
                'success': True,
                'number': result.get('number'),
                'sys_id': result.get('sys_id')
            })
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)})
        }
