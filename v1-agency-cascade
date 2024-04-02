import json
import csv
import os
import boto3
from botocore.exceptions import NoCredentialsError
import http.client
from botocore.exceptions import ClientError
from urllib.parse import urlparse
from datetime import datetime, timedelta, timezone

def get_secret():
    secret_name = "poll-patchwork"
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
        "email" : graphql_email,
        "password" : graphql_password
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
    
def find_shifts(token, creds):
    creds_dict = json.loads(creds)
    graphql_url = creds_dict["URL"]
    headers = {
        "Content-Type": "application/json",
        "X-Authorization": f"{token}"
    }
    
    graphql_query = """
      query Query($page: Int, $organisationIds: [Int!], $createdAt: DateTime, $limitToScope: ShiftScope) {
        shifts(page: $page, organisationIds: $organisationIds, createdAt: $createdAt, limitToScope: $limitToScope) {
          id
          startTime
          externalId
          department {
            departmentsPreference {
                cutOffAgency
                }
          }
          agencyRegistration {
            agency {
              title
            }
            user {
              fullName
            }
          }
          auditEvents {
            event
            time
          }
        }
      }
    """
    two_weeks_ago = datetime.now() - timedelta(days=14)
    created_at_date = two_weeks_ago.strftime("%Y-%m-%d")
    print(f"created at timestamp : {created_at_date}")
    gql_variables = {
      "createdAt": created_at_date,
      "organisationIds": [98],
      "limitToScope": "NO_SCOPE",
      "page": 1
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
    
def find_latest_agency_booking_time(agency_bookings):
    latest_event_times = {}

    for shift in agency_bookings:
        if 'auditEvents' in shift:
            for event in shift['auditEvents']:
                if event['event'] == 'Agency Booking Accepted':
                    shift_id = shift['id']
                    event_time = event['time']
                    if shift_id not in latest_event_times or event_time > latest_event_times[shift_id]:
                        latest_event_times[shift_id] = event_time
            
    return latest_event_times
    
def define_master_list(times, agency_bookings):
    master_list = []
    for shift in agency_bookings:
        shift_id = shift['id']
        if shift_id in times:
            booking_info_entry = {
                'shift_id': shift_id,
                'healthroster_id' : shift['externalId'],
                'shift_start_time': shift['startTime'],
                'agency_kickout': shift['department']['departmentsPreference']['cutOffAgency'],
                'agency_title': shift['agencyRegistration']['agency']['title'],
                'user_full_name': shift['agencyRegistration']['user']['fullName'],
                'event_time': times[shift_id]
            }
            master_list.append(booking_info_entry)
    
    return master_list
    
def define_agency_kickout_list(current_time, master_list):
    post_agency_kickout_master_list = []
    for shift in master_list:
        shift_start_time = datetime.fromisoformat(shift['shift_start_time']).replace(tzinfo=timezone.utc)
        agency_kickout_time = shift_start_time + timedelta(days=shift['agency_kickout'])

        if agency_kickout_time < current_time:
            print(f"Agency kick out time : {agency_kickout_time} is before (today): {current_time} and has passed")
            post_agency_kickout_master_list.append(shift)
        else:
            print(f"Invalid entry: {shift['shift_id']}")
            
    return post_agency_kickout_master_list

def write_to_csv(data, file_path):
    with open(file_path, mode='w', newline='') as csv_file:
        fieldnames = data[0].keys()
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)

def upload_to_s3(file_name, bucket, object_name):
    s3 = boto3.client('s3')
    existing_buckets = s3.list_buckets()['Buckets']

    if bucket not in [existing_bucket['Name'] for existing_bucket in existing_buckets]:
        s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={'LocationConstraint': 'eu-west-2'})
        print(f"S3 bucket created: {bucket}")
        
    try:
        s3.upload_file(file_name, bucket, object_name)
        print(f"File uploaded to S3: {object_name}")
    except Exception as e:
        print(f"An error occurred: {e}")

def lambda_handler(event, context):
    creds = get_secret()
    auth_data = generate_token(creds)
    parsed_auth_data = json.loads(auth_data)
    token = parsed_auth_data['data']['hubUserLogin']['token']
    shifts = find_shifts(token, creds)
    agency_bookings = [shift for shift in shifts if shift['agencyRegistration'] is not None and shift.get('externalId') is not None]
    
    booking_info = [
    {
        'shift_id': shift['id'],
        'healthroster_id' : shift['externalId'],
        'agency_title': shift['agencyRegistration']['agency']['title'],
        'user_full_name': shift['agencyRegistration']['user']['fullName'],
        'agency_kickout': shift['department']['departmentsPreference']['cutOffAgency'],
        'shift_start_time': shift['startTime']
    }
    for shift in agency_bookings
    ]
    
    times = find_latest_agency_booking_time(agency_bookings)
    master_list = define_master_list(times, agency_bookings)
    current_time = datetime.now(timezone.utc)
    post_agency_kickout_master_list = define_agency_kickout_list(current_time, master_list)
            
    file_name = f"/tmp/kch-{current_time.strftime('%d-%b-%Y %H:%M')}.csv"
    write_to_csv(post_agency_kickout_master_list, file_name)
    trimmed_path = file_name[5:]
    
    s3_bucket = 'kch-agency-kickout-bucket'
    s3_key = f"kings/agency/{trimmed_path}"
    upload_to_s3(file_name, s3_bucket, s3_key)
