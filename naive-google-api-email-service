import os
import re
import pickle
import base64
import email
import pandas as pd
import html2text
import ast
from datetime import datetime
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from bs4 import BeautifulSoup
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
import smtplib

SCOPES = ['https://www.googleapis.com/auth/gmail.readonly', 'https://www.googleapis.com/auth/gmail.send']


def identify_key_errors(row):
    conditions = {
    '[IGNORE]': '[IGNORE] 1',
    'No available errors': '[IGNORE] 1',
    'NONE IN SCOPE': '[IGNORE] 1',
    'no bankstaff id and no recgonised email header': '[IGNORE] 1',
    'Unable to find Agency Worker': '2 Unable to find agency worker',
    'Missing worker in BankStaff': '3 Worker not on Bankstaff',
    'Missing Worker on Patchwork': '4 Worker not on Patchwork',
    'Current BankStaff Staff': '5 Out of syncs',
    'Current BankStaff End': '6 Out of syncs',
    'Unable to find mapped reason_for_request': '7 Missing mapping reason for request',
    'Unable to update time': '8 Unable to change times',
    'not suitable for your staff': '9 Unsuitable staff group',
    'you can not assign worker': '9 Unsuitable staff group',
    "Unable to find '": '10 Unable to find agency on Patchwork',
    "mapping for '": '11 Missing grade mappings',
    'already has a shift booked at': '12 Unable to create/updates as worker already booked into conflicting shift in Patchwork',
    'cannot amend times': '13 Shift is approved on Patchwork, cannot update times',
    'Unable to cancel': '14 Unable to cancel workers in Patchwork',
    'has open booking': '15 Shift has booking applications present, cannot update in hub'
    }
    for key, value in conditions.items():
        if key in row['Errors']:
            return value

    return 0  # default case

def send_email(service, receiver_email, subject, body, attachment_file):
    print ("sending email")
    message = MIMEMultipart()
    message["To"] = receiver_email
    message["Subject"] = subject

    message.attach(MIMEText(body, "plain"))

    with open(attachment_file, "rb") as attachment:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
    encoders.encode_base64(part)
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= {os.path.basename(attachment_file)}",
    )
    message.attach(part)

    # Connect to Gmail API and send email
    message = (service.users().messages().send(userId='me', body={'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}).execute())

def main():
    print ("running main")
    email_data = []
    pages_processed = 0
    current_error = ''
    total_count = 0
    receiver_email = "benjamin.harry@patchwork.health"

    creds = None
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    try:
        service = build('gmail', 'v1', credentials=creds)
        # Use a query with date ranges (date format YYYY/MM/DD)
        query = "after:2023/06/14 before:2023/06/28"
        result = service.users().messages().list(userId='me', q=query).execute()
        messages = result.get('messages')
        while 'nextPageToken' in result:
            page_token = result['nextPageToken']
            result = service.users().messages().list(userId='me', q=query, pageToken=page_token).execute()
            messages.extend(result.get('messages'))
            pages_processed += 1
        for msg in messages:
            txt = service.users().messages().get(userId='me', id=msg['id']).execute()
            try:
                payload = txt.get('payload', {})
                errors = ''
                headers = payload['headers']
                subject = None
                for d in headers:
                    if d['name'] == 'Subject':
                        subject = d['value']
                        break
                if subject:
                    match = re.search(r'\b\d{10}\b', subject)
                    if match:
                        bankstaff_request_id = match.group()
                        date = [d['value'] for d in headers if d['name'] == 'Date'][0]
                        total_count += 1

                        if 'parts' in payload:
                            for part in payload['parts']:
                                mimeType = part.get('mimeType')
                                if mimeType == 'text/html':
                                    body_data = part['body'].get('data')
                                    if body_data:
                                        body = base64.urlsafe_b64decode(body_data).decode('utf-8')
                                        converter = html2text.HTML2Text()
                                        converter.ignore_links = True 
                                        body = converter.handle(body)
                        else:
                            mimeType = payload.get('mimeType')
                            if mimeType == 'text/html':
                                body_data = payload.get('body', {}).get('data')
                                if body_data:
                                    body = base64.urlsafe_b64decode(body_data).decode('utf-8')
                                    converter = html2text.HTML2Text()
                                    converter.ignore_links = True 
                                    body = converter.handle(body)
                        if "Unable To Create" in subject:
                            current_error = 'unable_to_create' 
                            error_message_match = re.search(r'"message"=>"(.*?)"', body.replace('\n', ' '))
                            if error_message_match:
                                errors = error_message_match.group(1)
                        elif f"Unable To Update {bankstaff_request_id} On BankStaff" in subject:
                            current_error = 'unable_to_update_on_bankstaff'
                            error_message_match = re.search(r":error_message=>\"(.*?)\",", body, re.DOTALL)
                            if error_message_match:
                                errors = error_message_match.group(1)
                            else:
                                fallback_match = re.search(r"error_message=>[\"']?(.*?)[\"']?\}", body)
                                if fallback_match:
                                    errors = fallback_match.group(1)
                                else:
                                    second_fallback_match = re.search(r":error_message=>[\"']?([^\"'}]+)[\"'}]?,", body)
                                    if error_message_match:
                                        errors = second_fallback_match.group(1)
                                    else:
                                        errors = "[IGNORE] No error message found, likely a pop up"
                        elif "Unable To Update BankStaff" in subject and "in Hub" in subject:
                            current_error = 'unable_to_update_on_patchwork'
                            errors_section = re.search(r'(?s)"errors"=>\[\{.*?}\]\}\}', body)
                            if errors_section:
                                errors_text = errors_section.group()
                                errors_text = errors_text.replace('=>', ':').replace('`', '').strip('"')
                                try:
                                    errors = ast.literal_eval(errors_text)
                                except (ValueError, SyntaxError):
                                    errors = re.search(r'"message"\s*:\s*"(.+?)"', errors_text)
                                    if errors:
                                        errors = errors.group(1)
                                    else:
                                        fallback_match = re.search(r'(?<="message":")[^"]*', errors_text)
                                        if fallback_match:
                                            errors = fallback_match.group(0)
                                        else:
                                            errors = f'[ERROR] Unable to parse JSON: {errors_text}'
                        elif "Worker Not Found in BankStaff" in subject:
                            current_error = 'unable_to_find_worker'
                            first_name_match = re.search(r'First Name:\s*(.+)', body, re.IGNORECASE)
                            last_name_match = re.search(r'Last Name:\s*(.+)', body, re.IGNORECASE)
                            worker_grade_match = re.search(r'Worker Grade:\s*(.+)', body, re.IGNORECASE)
                            assignment_number_match = re.search(r'Assignment Number:\s*(.+)', body, re.IGNORECASE)
                            ni_number_match = re.search(r'NI Number:\s*(.+)', body, re.IGNORECASE)
                            dob_match = re.search(r'Date of Birth:\s*(.+)', body, re.IGNORECASE)

                            first_name = first_name_match.group(1) if first_name_match else ''
                            last_name = last_name_match.group(1) if last_name_match else ''
                            worker_grade = worker_grade_match.group(1) if worker_grade_match else ''
                            assignment_number = assignment_number_match.group(1) if assignment_number_match else ''
                            ni_number = ni_number_match.group(1) if ni_number_match else ''
                            dob = dob_match.group(1) if dob_match else ''

                            error_message = "Missing worker in BankStaff: "
                            error_message += f"First Name: {first_name}, "
                            error_message += f"Last Name: {last_name}, "
                            error_message += f"Worker Grade: {worker_grade}, "
                            error_message += f"Assignment Number: {assignment_number}, "
                            error_message += f"NI Number: {ni_number}, "
                            error_message += f"Date of Birth: {dob}"

                            errors = error_message
                        elif "RPA Intervention Required for BankStaff" in subject and "created in Hub" in subject:
                            if "Unable to assign Worker during create of BankStaff" in body:
                                current_error = 'intervention_required_missing_pw_worker'
                                last_name_match = re.search(r"Last Name, First Name:\*\* ([\w']+ [\w']+)", body)
                                assignment_number_match = re.search(r"Assignment Number:\*\* (\d+)", body)

                                if last_name_match and assignment_number_match:
                                    name = last_name_match.group(1)
                                    last_name, first_name = name.split(' ')
                                    assignment_number = assignment_number_match.group(1)
                                    errors = f'Missing Worker on Patchwork, please reconcile {last_name}, {first_name}, {assignment_number}'
                                else:
                                    errors = "No matching values found in the body"
                            else:
                                errors = "No error message found"
                        else:
                            current_error = 'merged or irrelevant'
                            errors = 'No available errors, ignore or investigate'
                    elif "Unable To Delete  On BankStaff" in subject:
                        current_error = 'unable_to_delete'
                        bankstaff_request_id = 'NONE FOUND'
                        errors = 'NONE IN SCOPE - Shift deleted on Patchwork'
                        hub_id_match = re.search(r'hub_id=>(\d+)', body)
                        if hub_id_match:
                            extracted_hub_id = hub_id_match.group(1)
                            bankstaff_request_id = f"HUB_ID_ONLY-{extracted_hub_id}"
                    else:
                        errors  = 'no bankstaff id and no recgonised email header'
                email_data.append({'ID': bankstaff_request_id, 'Date of Email': date, 'High level error' : current_error, 
                                    'Errors': str(errors), 'Subject': subject}) # re add 'Body' : body, if you need to see the body of the email
            except Exception as e:
                print("Unable to read: ", str(e))

    except Exception as error:
        print(f'An error occurred: {error}')

    df = pd.DataFrame(email_data)
    df.index.name = 'Index'
    df['KEY'] = df.apply(identify_key_errors, axis=1)
    filename = 'STHK-EXCEPTIONS-' + datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + '.csv'
    df.to_csv(filename)
    subject = filename
    body = "Please find your csv attached"
    send_email(service, receiver_email, subject, body, filename)

def handler(event, context):
    print("calling lambda handler")
    main()
