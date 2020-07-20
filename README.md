# Keyword Rank Tracker

This script:
1. reads keywords and target URLs from a Google Sheet
2. checks the keyword's rank on SERP pages using DataForSEO
3. writes the target url's rank back to the spreadsheet

## Usage

    python3 keyword_rank_tracker.py

This script is designed to be run once a month via crontab. Since DataForSEO's API is asynchronous, the script must be run once to 
initiate the task and then again to collect the results. An hour between executions should be enough to guarantee all tasks return.   
