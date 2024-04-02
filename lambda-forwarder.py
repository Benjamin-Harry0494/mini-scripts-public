import json
from urllib.parse import urlparse
import os
import http.client
import boto3
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    url = 'https://api.patchwork.health'
    headers = []
    results = []
    
    try:
        post_headers = set_headers(headers, event, url)
        parsed_url = urlparse(url)
        hostname = parsed_url.hostname
        conn = http.client.HTTPSConnection(hostname)
        path =  "/allocate-inbound"
        json_string = json.dumps(event)
        event_body = json_string.encode('utf-8')
        print(f"url: {url}, path: {path}, body: {event_body}, headers: {post_headers}")
        conn.request("POST", path, body=event_body, headers=post_headers)
        response = conn.getresponse()
        response_data = response.read()
        status_code = response.status
        results.append( {
            "url": url,
            "status_code": status_code,
            "message": response_data
        }   )
        print(response_data.decode("utf-8"))
    except Exception as e:
        print(f"Error: {str(e)}")
        return(f"Error: {str(e)}")
    
    formatted_results = []
    for result in results:
        formatted_result = {
            "url": result['url'],
            "statusCode": result['status_code'],
            "message": result['message'].decode('utf-8') 
        }
        formatted_results.append(formatted_result)
    
    return {
        "statusCode": 202,
        "headers": {
            "Content-Type": "application/json"  # Specify JSON content type
        },
        "body": json.dumps({
            "messages": formatted_results
        })
    }
    
def set_headers(headers, event, url):

    post_headers = {} 
    post_headers['x-authorization'] = setXAuth(event)
    post_headers['content-type'] = 'application/json'
    post_headers['user-agent'] = 'lambda-forwarder'

    return post_headers

    
def retrieve_creds(org):
    secret_name = f"{org}_Creds"
    region_name = "eu-west-2"
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        print(f"Error: {str(e)}")
        raise e

    return get_secret_value_response['SecretString']
    
def generate_token(creds):
    creds_dict = json.loads(creds)
    graphql_url = 'https://api.patchwork.health/graphql'
    graphql_password = creds_dict["Password"]
    graphql_email = creds_dict["Email"]

    graphql_query = """
    mutation hubUserLogin ($email: String!, $password: String!) {
        hubUserLogin (email: $email, password: $password) {
            refreshToken
            token
        }
    }
    """

    gql_variables = {
        "email": graphql_email,
        "password": graphql_password
    }

    headers = {
        "Content-Type": "application/json"
    }

    gql_request = {
        "query": graphql_query,
        "variables": gql_variables
    }

    parsed_url = urlparse(graphql_url)
    hostname = parsed_url.hostname

    gql_request_json = json.dumps(gql_request)
    conn = http.client.HTTPSConnection(hostname)
    conn.request("POST", "/graphql", gql_request_json, headers)
    response = conn.getresponse()
    response_data = response.read().decode("utf-8")
    conn.close()
    return response_data
    
    
def get_token(org):
    creds = retrieve_creds(org)
    auth_data = generate_token(creds)
    parsed_auth_data = json.loads(auth_data)
    return parsed_auth_data['data']['hubUserLogin']['token']
    

def setXAuth(event):
    if 'trustCodes' in event and event['trustCodes'] is not None:
        trust_code = event['trustCodes'][0]
        print(f"Using recognised trust codes: {trust_code}")
    else:
        print("no trustcodes found, using default")
        trust_code = 'Default'
    
    cases = {
        "RSCH": lambda: setRSCH(),
        "KCHTRIAL": lambda: setKCH(),
        "Default": lambda: setDefault()
    }
    
    auth_token = cases.get(trust_code)()
    print(auth_token)
    return auth_token.strip("'")
    
    
def setKCH():
    return get_token('KCH')

def setRSCH():
    return get_token('RSCH')
    
def setDefault():
    return get_token('Default')
    
