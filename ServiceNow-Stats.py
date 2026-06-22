import json
import requests
import os

def lambda_handler(event, context):
    url = f"{os.environ['SERVICENOW_URL']}/api/now/table/incident"
    auth = (os.environ['SERVICENOW_USER'], os.environ['SERVICENOW_PASSWORD'])
    
    total_res = requests.get(url, auth=auth, params={'sysparm_limit': '1'})
    total = int(total_res.headers.get('X-Total-Count', 0))
    
    open_res = requests.get(url, auth=auth, params={'sysparm_query': 'state=1', 'sysparm_limit': '1'})
    open_count = int(open_res.headers.get('X-Total-Count', 0))
    
    resolved_res = requests.get(url, auth=auth, params={'sysparm_query': 'state=3', 'sysparm_limit': '1'})
    resolved_count = int(resolved_res.headers.get('X-Total-Count', 0))
    
    return {
        'statusCode': 200,
        'headers': {'Access-Control-Allow-Origin': '*'},
        'body': json.dumps({
            'total': total,
            'open': open_count,
            'resolved': resolved_count
        })
    }
