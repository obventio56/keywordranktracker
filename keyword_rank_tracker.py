from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from client import RestClient
from urllib.parse import urlparse
import json
import sys
from pprint import pprint
import datetime

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

# The ID and range of a sample spreadsheet.
SPREADSHEET_ID = '14dHPV1nQYuTL6voAlgbwrQLvUAmm22drSW0-0x9SraI'

today = datetime.date.today()
datestring = today.strftime('%m/%d/%Y')

def colnum_string(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

def authenticate_google_sheets():
    creds = None
    # The file token.pickle stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists('token.pickle'):
        with open('token.pickle', 'rb') as token:
            creds = pickle.load(token)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                'credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    return build('sheets', 'v4', credentials=creds)

def authenticate_dataforseo():
    creds = None
    with open("dataforseocreds.json", 'r') as f:
        creds = json.load(f)
    
    if creds:
        return RestClient(creds["login"], creds["password"])
    else: 
        raise ValueError('No datafroseo credentials.')


def load_keyword_targets(service):
   
    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range='2020!A:B').execute()
    values = result.get('values', [])

    if not values:
        raise ValueError('No target data found.')
    else:
        return values[1:len(values)]

def initiate_tasks(keyword_targets, client):
    post_data = dict()
    for _, target in enumerate(keyword_targets):
        post_data[len(post_data)] = dict(
            language_code="en",
            location_code=2840,
            keyword=target[0]
        )

    response = client.post("/v3/serp/google/organic/task_post", post_data)
    if response["status_code"] != 20000:
        print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))


def fetch_completed_tasks(client):
    response = client.get("/v3/serp/google/organic/tasks_ready")
    if response['status_code'] == 20000:
        results = dict()
        pprint(response)
        for task in response['tasks']:
            if (task['result'] and (len(task['result']) > 0)):
                for resultTaskInfo in task['result']:
                    if (resultTaskInfo['endpoint_regular']):

                        task_result = client.get(resultTaskInfo['endpoint_regular'])    
                        keyword = task_result["tasks"][0]["data"]["keyword"]
                        items = task_result["tasks"][0]["result"][0]["items"] 
                        results[keyword] = items
                        pprint(items)
                        
        return results
    else:
        print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))

def get_rank(targets, results):
    rank_results = []
    for  _, target in enumerate(targets):
        serp_entries = results[target[0]]
        
        a = next((i for i in serp_entries if i["url"] == target[1]), -1)
        if a != -1:
            rank_results.append([a["rank_absolute"], ""])
            continue
        domain_name = urlparse(target[1]).netloc
        a = next((i for i in serp_entries if i["domain"] == domain_name), -1)
        if a != -1:
            rank_results.append([a["rank_absolute"], a["url"]])
            continue
        rank_results.append([-1, ""])
    return rank_results

def write_rank_results(rank_results, service):
    rank_results.insert(0,[datestring, "Ranking URL"])

    #get sheet id and count existing columns
    request = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID, ranges='2020!1:1')
    response = request.execute()
    sheet_id = response["sheets"][0]["properties"]["sheetId"]

    column_count = response["sheets"][0]["properties"]["gridProperties"]["columnCount"]

    
    batch_update_spreadsheet_request_body = {"requests": [
        {
        "appendDimension": {
            "sheetId": sheet_id,
            "dimension": "COLUMNS",
            "length": 2
        }
        }
    ]}

    request = service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=batch_update_spreadsheet_request_body)
    response = request.execute()

    body = {
        'values': rank_results
    }
    
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, range=('2020!' + str(colnum_string(column_count + 1)) + ":" + str(colnum_string(column_count + 2)))
        , valueInputOption='RAW', body=body).execute()


def start_job():
    service = authenticate_google_sheets()
    targets = load_keyword_targets(service)
    client = authenticate_dataforseo()
    initiate_tasks(targets, client)

    with open('targets.pickle', 'wb') as targetsf:
        pickle.dump(targets, targetsf)

def collect_results():
    service = authenticate_google_sheets()
    client = authenticate_dataforseo()
    targets = []
    with open('targets.pickle', 'rb') as targetsf:
        targets = pickle.load(targetsf)

   
    results = fetch_completed_tasks(client)
    rank_results = get_rank(targets, results)
    
    write_rank_results(rank_results, service)

if __name__ == '__main__':
    if sys.argv[1] == 'init':
        start_job()
    if sys.argv[1] == 'collect': 
        collect_results()