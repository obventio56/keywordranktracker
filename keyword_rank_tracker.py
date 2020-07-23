# Drive API key: AIzaSyAHPTev5BZN8cVVSvoYWfUe-SLZ_9IacRI 


from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from apiclient import errors
from client import RestClient
from urllib.parse import urlparse
import json
import sys
from pprint import pprint
import datetime

# Tells Google oauth what we want permission to touch
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

# Target spreadsheet ID
FOLDER_ID = '1gW7KFG6374MLzd_r7J82MuiVdSuytTBM'
SHEET_TITLE = str(datetime.datetime.now().year)

today = datetime.date.today()
datestring = today.strftime('%m/%d/%Y')

# Convert column index into A1 notation
def colnum_string(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

def authenticate_google(API_title, API_version):
    creds = None
    if os.path.exists(API_title + '_token.pickle'):
        with open(API_title + '_token.pickle', 'rb') as token:
            creds = pickle.load(token)
            
    # If there are no valid credentials available, prompt log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(
                API_title + '_credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(API_title + '_token.pickle', 'wb') as token:
            pickle.dump(creds, token)
    
    return build(API_title, API_version, credentials=creds)

def authenticate_dataforseo():
    creds = None
    with open("dataforseocreds.json", 'r') as f:
        creds = json.load(f)
    
    if creds:
        return RestClient(creds["login"], creds["password"])
    else: 
        raise ValueError('No datafroseo credentials.')

    
def check_year_and_copy(service):
    spreadsheet_data = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID).execute()
    sheets = spreadsheet_data["sheets"]
    if next((sheet for sheet in sheets if sheet['properties']['title'] == SHEET_TITLE), -1) == -1:
        old_sheet = str(datetime.datetime.now().year - 1)
        old_sheet_id = next((sheet for sheet in sheets if sheet['properties']['title'] == old_sheet), -1)['properties']['sheetId']

        # Count rows we must copy
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                    range=(old_sheet + '!A:B')).execute()
        row_count = len(result.get('values', []))

        # Create a new sheet
        new_sheet_body = {
            "requests": [
                {
                "addSheet": {
                    "properties": {
                    "title": SHEET_TITLE,
                    "gridProperties": {
                        "rowCount": row_count,
                        "columnCount": 2
                    }
                    }
                }
                }
            ]
        }
        request = service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=new_sheet_body)
        new_sheet_response = request.execute()
        new_sheet_id = new_sheet_response["replies"][0]["addSheet"]["properties"]["sheetId"]

        # Copy targets from old sheet to new sheet
        copy_keywords_body = {
            "requests": [
                {
                "copyPaste": {
                    "source": {
                    "sheetId": old_sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": row_count,
                    "startColumnIndex": 0,
                    "endColumnIndex": 2
                    },
                    "destination": {
                    "sheetId": new_sheet_id,
                    "startRowIndex": 0,
                    "endRowIndex": row_count,
                    "startColumnIndex": 0,
                    "endColumnIndex": 2
                    },
                    "pasteType": "PASTE_NORMAL"
                }
                }
            ]
        }

        request = service.spreadsheets().batchUpdate(spreadsheetId=SPREADSHEET_ID, body=copy_keywords_body)
        request.execute()


# Download keywords from spreadsheet.
def load_keyword_targets(service):

    # If it's a new year then move to a new sheet
    check_year_and_copy(service)
   
    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=SPREADSHEET_ID,
                                range=(SHEET_TITLE + '!A:B')).execute()
    values = result.get('values', [])

    if not values:
        raise ValueError('No target data found.')
    else:
        return values[1:len(values)]

# Submit keywords as jobs to DataForSEO
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

        # Gymnastics that put SERP results in a dict with the keyword string as the key
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

# Search results for target url
def get_rank(targets, results):
    rank_results = []
    for  _, target in enumerate(targets):
        serp_entries = results[target[0]]
        
        # Find first index where the SERP url equals target url
        a = next((i for i in serp_entries if i["url"] == target[1]), -1)
        if a != -1:
            rank_results.append([a["rank_absolute"], ""])
            continue

        # If url search comes up empty, search by domain name
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
    request = service.spreadsheets().get(spreadsheetId=SPREADSHEET_ID, ranges=(SHEET_TITLE + '!1:1'))
    response = request.execute()
    sheet_id = response["sheets"][0]["properties"]["sheetId"]
    column_count = response["sheets"][0]["properties"]["gridProperties"]["columnCount"]

    # Insert 2 columns to the end of the spreadsheet to store our results
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

    # Write results to the two new columns we created
    body = {
        'values': rank_results
    }
    service.spreadsheets().values().update(
        spreadsheetId=SPREADSHEET_ID, range=(SHEET_TITLE + '!' + str(colnum_string(column_count + 1)) + ":" + str(colnum_string(column_count + 2)))
        , valueInputOption='RAW', body=body).execute()

# First step: download keywords from Google Sheets and send to DataForSEO 
def initiate_ranking():

    service = authenticate_google('drive', 'v3')
    page_token = None
    while True:
        response = service.files().list(q="mimeType='application/vnd.google-apps.spreadsheet' and '" + FOLDER_ID + "' in parents",
                                            spaces='drive',
                                            fields='nextPageToken, files(id)',
                                            pageToken=page_token).execute()
        for file in response.get('files', []):
            # Process change
            sheed_id = file.get('id')
            
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break



    
    #targets = load_keyword_targets(service)
    #client = authenticate_dataforseo()
    #initiate_tasks(targets, client)
    #with open('targets.pickle', 'wb') as targetsf:
    #    pickle.dump(targets, targetsf)

# Second step: once we're confident DataForSEO has completed our tasks, pull the results and write back to the spreadsheet
def collect_results():
    service = authenticate_google_sheets()
    client = authenticate_dataforseo()
    targets = []
    with open('targets.pickle', 'rb') as targetsf:
        targets = pickle.load(targetsf)
    results = fetch_completed_tasks(client)
    rank_results = get_rank(targets, results)
    write_rank_results(rank_results, service)

    os.remove("targets.pickle") 

if __name__ == '__main__':

    # if we're waiting on results, collect and save
    if os.path.exists('targets.pickle'):
        collect_results()
    # otherwise initiate new tasks
    else:
        initiate_ranking()