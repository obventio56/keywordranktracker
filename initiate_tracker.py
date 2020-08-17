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

# NOTE: If you're looking for a place to start reading, jump to "call_initiate_ranking"
# at the bottom of this file. 

# Tells Google oauth what we want permission to touch
SCOPES = ['https://www.googleapis.com/auth/spreadsheets',
          'https://www.googleapis.com/auth/drive']

SERVICE_ACCOUNT_FILE = 'service_account.json'

# Target spreadsheet ID
FOLDER_ID = '1_PCARCXUY2s4ROn1xUn-FiDBULp5ylCW'
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


# Make sure a sheet exists for the current year and
# copy keywords/URLs to new sheet if not (only runs at begining of each year)
def check_year_and_copy(sheet_id, service):

    # Get list of sheets in the spreadsheet
    spreadsheet_data = service.spreadsheets().get(spreadsheetId=sheet_id).execute()
    sheets = spreadsheet_data["sheets"]

    # the "next" function in this condition finds the first element of the array that matches our constraint
    # in this case that the sheet title matches the current year. 
    # If no sheet is found, it returns -1 and we create a sheet.
    if next((sheet for sheet in sheets if sheet['properties']['title'] == SHEET_TITLE), -1) == -1:

        old_sheet = str(datetime.datetime.now().year - 1)
        old_sheet_id = next((sheet for sheet in sheets if sheet['properties']['title'] == old_sheet), -1)['properties']['sheetId']

        # Count rows we must copy
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=sheet_id,
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
        request = service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=new_sheet_body)
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

        request = service.spreadsheets().batchUpdate(spreadsheetId=sheet_id, body=copy_keywords_body)
        request.execute()


# Download keywords from spreadsheet.
def load_keyword_targets(sheet_id, service):

    # If it's a new year then move to a new sheet
    check_year_and_copy(sheet_id, service)
   
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

# Submit keywords as jobs to DataForSEO
def initiate_tasks(keyword_targets, client):
    post_data = dict()
    for count, target in enumerate(keyword_targets):
        post_data[len(post_data)] = dict(
            language_code="en",
            location_code=2840,
            keyword=target[0]
        )
        # Rate limiting, no more than 100 tasks per post by DataForSEO rules
        if (count % 95 == 0 and count > 0) or count == len(keyword_targets) - 1:
            response = client.post("/v3/serp/google/organic/task_post", post_data)
            if response["status_code"] != 20000:
                print("error. Code: %d Message: %s" % (response["status_code"], response["status_message"]))
            post_data = dict()

# The "main" function
def initiate_ranking(_):

    # Authenticate our APIs
    client = authenticate_dataforseo()
    drive_service = authenticate_google('drive', 'v3')
    sheet_service = authenticate_google('sheets', 'v4')

    # List of Keyword/URL pairs to track:
    targets = [] 

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
            pprint(file.get('id'))

            # Add Keyword/URL pairs from this sheet to our list
            targets = targets + load_keyword_targets(file.get('id'), sheet_service)

            # Sleep to avoid hitting API rate limiting.
            time.sleep(20)

        # Request next "page" of spreadsheets    
        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break
        
    # Submit keyword/URL pairs to DataforSEO
    initiate_tasks(targets, client)

# This function wraps our script in a try/catch because otherwise GCP won't print errors if it crashes.
def call_initiate_ranking(_):
    try:
        initiate_ranking(None)
    except Exception as e:
        track = traceback.format_exc()
        print(track)