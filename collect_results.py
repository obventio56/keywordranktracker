# Basic system packages:
import traceback
import os.path
import os
import shutil
from urllib.parse import urlparse
import json
import sys
from pprint import pprint
import datetime
from pathlib import Path
import pathlib
import time

# Google APIs
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.oauth2 import service_account
from google.auth.transport.requests import Request

# DataforSEO:
from apiclient import errors
from client import RestClient

# NOTE: If you're looking for a place to start reading, jump to "call_collect_results"
# at the bottom of this file. 

# Tells Google oauth what we want permission to touch
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

# Google credentials file name
SERVICE_ACCOUNT_FILE = 'service_account.json'

# Target spreadsheet ID
FOLDER_ID = '1_PCARCXUY2s4ROn1xUn-FiDBULp5ylCW'

# Target sheet title (AKA the current year).
SHEET_TITLE = str(datetime.datetime.now().year)


# Convert column index into A1 notation
def colnum_string(n):
    string = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        string = chr(65 + remainder) + string
    return string

# Pass credentials from service_account.json to the Google API.
def authenticate_google(API_title, API_version):
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    return build(API_title, API_version, credentials=creds)

# Pass credentials from dataforseocreds.json to the DataForSEO API.
def authenticate_dataforseo():
    creds = None
    with open("dataforseocreds.json", 'r') as f:
        creds = json.load(f)
    
    if creds:
        return RestClient(creds["login"], creds["password"])
    else: 
        raise ValueError('No datafroseo credentials.')

# Search results for target url
def get_rank(targets, results):
    rank_results = []
    for  _, target in enumerate(targets):
        serp_entries = results[target[0]]
        
        # Find first index where the SERP url equals target url
        # The "next" function in this condition finds the first element of the array that matches our constraint
        a = next((i for i in serp_entries if i["url"] == target[1]), -1)
        if a != -1:
            rank_results.append([a["rank_absolute"], a["url"]])
            continue

        # If url search comes up empty, search by domain name
        domain_name = urlparse(target[1]).netloc
        a = next((i for i in serp_entries if i["domain"] == domain_name), -1)
        if a != -1:
            rank_results.append([a["rank_absolute"], a["url"]])
            continue
        rank_results.append([-1, ""])
    return rank_results

def write_rank_results(rank_results, spreadsheet_id, service):

    # Find the current date and prepend it to the values list for the column header:
    today = datetime.date.today()
    datestring = today.strftime('%m/%d/%Y')
    rank_results.insert(0,[datestring, "Ranking URL"])

    # Get sheet id and count existing columns (so we know where to insert new columns).
    request = service.spreadsheets().get(spreadsheetId=spreadsheet_id, ranges=(SHEET_TITLE + '!1:1'))
    response = request.execute()
    column_count = response["sheets"][0]["properties"]["gridProperties"]["columnCount"]
    sheet_id = response["sheets"][0]["properties"]["sheetId"]

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
    request = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=batch_update_spreadsheet_request_body)
    response = request.execute()

    # Write results to the two new columns we created
    body = {
        'values': rank_results
    }
    response = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id, range=(SHEET_TITLE + '!' + str(colnum_string(column_count + 1)) + ":" + str(colnum_string(column_count + 2)))
        , valueInputOption='RAW', body=body).execute()
    

# Download all completed tasks from the DataForSEO API.
def fetch_completed_tasks(client):
    response = client.get("/v3/serp/google/organic/tasks_ready")

    # If no reqeust errors
    if response['status_code'] == 20000:

        # Dictionary to store our results
        results = dict()

        # Gymnastics that put SERP results in a dict with the keyword string as the key
        for task in response['tasks']:
            if (task['result'] and (len(task['result']) > 0)):
                for resultTaskInfo in task['result']:
                    if (resultTaskInfo['endpoint_regular']):

                        # The "/tasks_ready" endpoint returns a list of "tasks" but not the actual results
                        # so we have to make a call for those one by one.
                        task_result = client.get(resultTaskInfo['endpoint_regular'])    
                        keyword = task_result["tasks"][0]["data"]["keyword"]
                        items = task_result["tasks"][0]["result"][0]["items"] 

                        # Put the results in our dictionary, using the keyword string as our index
                        results[keyword] = items
                        
        return results
    else:
        print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))

# Download keywords from spreadsheet.
def load_keyword_targets(sheet_id, service):
   
    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=sheet_id,
                                range=(SHEET_TITLE + '!A:B')).execute()
    values = result.get('values', [])

    if not values:
        raise ValueError('No target data found.')
    else:

        # Cut off first element because we don't want column titles
        return values[1:len(values)]


# Second step: once we're confident DataForSEO has completed our tasks, pull the results and write back to the spreadsheet
def collect_results(_):

    # Authenticate our APIs
    drive_service = authenticate_google('drive', 'v3')
    sheet_service = authenticate_google('sheets', 'v4')
    client = authenticate_dataforseo()

    # Download SERP results from DataForSEO
    results = fetch_completed_tasks(client)

    # Loop through spreadsheets in our folder until there are no more. 
    # This while loop is necessary to handle (potentially infintie) pagination from Google Drive
    page_token = None
    while True:

        # Request one "page" of spreadsheets
        response = drive_service.files().list(q="mimeType='application/vnd.google-apps.spreadsheet' and '" + FOLDER_ID + "' in parents",
                                            spaces='drive',
                                            fields='nextPageToken, files(id)',
                                            pageToken=page_token, supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        
        # Loop through each spreadsheet in this "page"
        for file in response.get('files', []):

            # Pull keywords/URLs from Google Sheets
            targets = load_keyword_targets(file.get('id'), sheet_service)

            # Find where each URL ranks in our SERP results
            rank_results = get_rank(targets, results)
            
            # Write the rank info back to the Google Sheet.
            write_rank_results(rank_results, file.get('id'), sheet_service)

            # Sleep to avoid hitting API rate limiting.
            time.sleep(20)

        # Request next "page" of spreadsheets    
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break

# This function wraps our script in a try/catch because otherwise GCP won't print errors if it crashes.
def call_collect_results(_):
    try:
        collect_results(None)
    except Exception as e:
        track = traceback.format_exc()
        print(track)

