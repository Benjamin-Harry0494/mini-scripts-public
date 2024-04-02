import json
import csv
import os
import boto3
from botocore.exceptions import NoCredentialsError
import http.client
from botocore.exceptions import ClientError
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone


graphql_url = None
graphql_password = None
graphql_email = None


def get_creds():
    secret_name = "Staging-Cascade"
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
        print(f"Error: {e}")
        raise e

    return get_secret_value_response['SecretString']
    

def generate_token(creds):
    global graphql_url, graphql_password, graphql_email
    
    creds_dict = json.loads(creds)
    graphql_url = creds_dict["URL"]
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
    

def find_shifts(token):
    headers = {
        "Content-Type": "application/json",
        "X-Authorization": f"{token}"
    }

    graphql_query = """
    query Query($page: Int, $limitToScope: ShiftScope, $fromStartTime: DateTime, $organisationId: Int, $toStartTime: DateTime) {
      shifts(page: $page, limitToScope: $limitToScope, fromStartTime: $fromStartTime, organisationId: $organisationId, toStartTime: $toStartTime) {
        id
        status
        startTime
        externalId
        sentToAgency
        bookedBy {
          agencyRegistrations {
            user {
              fullName
            }
          }
          fullName
        }
      }
    }
    """
    two_weeks_ago = datetime.now() - timedelta(days=14)
    two_weeks_forward = datetime.now() + timedelta(days=14)
    from_date = two_weeks_ago.strftime("%Y-%m-%d")
    to_date = two_weeks_forward.strftime("%Y-%m-%d")
    print(f"searching for shfits from {from_date}, to {to_date}")
    gql_variables = {
      "page": 1,
      "limitToScope": "NO_SCOPE",
      "fromStartTime": from_date,
      "organisationId": 98,
      "toStartTime": to_date
    }

    all_results = []
    while True:
        gql_variables["page"] = gql_variables["page"]

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

        response_json = json.loads(response_data)
        shifts = response_json.get("data", {}).get("shifts", [])
        all_results.extend(shifts)

        if not shifts:
            print(f"no more shifts at page number : {gql_variables['page']}")
            break
        else:
            gql_variables["page"] += 1

    return all_results
    
    
def find_agency_shifts(shifts, token):
    today = datetime.now(timezone.utc)
    for shift in shifts:
        if isinstance(shift['shift_start_time'], str):
            shift_start_time = datetime.fromisoformat(shift['shift_start_time']).replace(tzinfo=timezone.utc)
        else:
            shift_start_time = shift['shift_start_time']
        
        tier_one_cascade = today + timedelta(days=int(os.environ.get('tier_one_days')))
        if today <= shift_start_time <= tier_one_cascade:
            cascade(shift, today, shift_start_time, token)
        else:
            print(f"Shift: {shift['shift_id']} is not in the agency period")
            

def cascade(shift, today, shift_start_time, token):
    print(f"current shift_start_time is : {shift_start_time}")
    tier_two_cascade = today + timedelta(days=int(os.environ.get('tier_two_days')))
    if today <= shift_start_time <= tier_two_cascade:
        cascade_to_tier(shift['shift_id'], 2, token)
    else:
        cascade_to_tier(shift['shift_id'], 1, token)


def cascade_to_tier(shift_id, tier, token):
    print(f"Shift {shift_id} is being sent to agency tier {tier}")
    graphql_mutation = """
    mutation SendToAgencyTier($shiftId: Int!, $agencyTierId: Int) {
      sendToAgencyTier(shiftId: $shiftId, agencyTierId: $agencyTierId) {
        success
      }
    }
    """
    headers = {
        "Content-Type": "application/json",
        "X-Authorization": f"{token}"
    }
    
    graphql_variables = {
      "shiftId": shift_id,
      "agencyTierId": None
    }
    
    if tier == 1:
        graphql_variables["agencyTierId"] = 165
    elif tier == 2:
        graphql_variables["agencyTierId"] = 177
    else:
        print("invalid tier provided")
        
    gql_request = {
        "query": graphql_mutation,
        "variables": graphql_variables
    }
    
    parsed_url = urlparse(graphql_url)
    hostname = parsed_url.hostname

    gql_request_json = json.dumps(gql_request)
    conn = http.client.HTTPSConnection(hostname)
    conn.request("POST", "/graphql", gql_request_json, headers)
    response = conn.getresponse()
    response_data = response.read().decode("utf-8")
    conn.close()

    response_json = json.loads(response_data)
        

def lambda_handler(event, context):
    creds = get_creds()
    auth_data = generate_token(creds)
    parsed_auth_data = json.loads(auth_data)
    token = parsed_auth_data['data']['hubUserLogin']['token']
    
    shifts = find_shifts(token)
    empty_shifts = [shift for shift in shifts if
                    shift.get('status') == 'AVAILABLE' and
                    shift.get('externalId') is not None]
                    
    # We now have all shifts that are 1. not booked and 2. exist in the API
    # The next step is to check against their start time
    # In this example I will say that sending to agency tier 1 happens 7 days before the shift start time
    # I will send to agency tier 2 once we are 3 days before the shift start time
    
    for shift in empty_shifts:
        print(f"shift: {shift}")
    
    shift_info = [
        {
            'shift_id': shift['id'],
            'shift_start_time': shift['startTime']
        }
        for shift in empty_shifts
    ]
    
    agency_shifts = find_agency_shifts(shift_info, token)
    
