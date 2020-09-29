#!/usr/bin/env python
# coding: utf-8
"""
Upload scraper results and log files to Dropbox
"""

# Import packages
import argparse
import dropbox
import os
import re
import pandas as pd
import datetime
from shutil import copyfile


# Helper function to get the Dropbox token
def get_dropbox_token(token_value = None):
    if token_value:
        token = token_value
    else:
        parser = argparse.ArgumentParser(description='Upload files to Dropbox')
        parser.add_argument('--dropbox_token', type=str, metavar='KEY', 
            action='store', help='Token for Dropbox authentication')
        opts = parser.parse_args()
        token = opts.dropbox_token


    if token:
        return token
    else:
        print('Invalid Drobox token specification')
        return None



# Helper function(s)
def dropbox_d4bl_upload(file_from, dropbox_folder, timestamp):
    if dropbox_token is None:
        print('Please provide a valid Dropbox token.')
        return None

    try:
        dbx = dropbox.Dropbox(dropbox_token)
        filename = file_from.split('/')[-1]
        print(filename)
        file_components = filename.split('.')
        file_ts = "{}_{}.{}".format(file_components[0], timestamp, file_components[1])
        file_to = '/{}/{}'.format(dropbox_folder, file_ts)
        print(file_to)
        with open(file_from, 'rb') as f:
                    dbx.files_upload(f.read(), file_to, mode=dropbox.files.WriteMode.overwrite)
        print("File '{}' successfully uploaded to Dropbox".format(file_ts))
    except Exception as inst:
        print('Dropbox upload error.')
        print(inst)



# Helper function to find most recent file in a directory that matches a given search string
def get_recent_file(file_path = 'output/csv', search_string='^covid_disparities_output_\d{4}-\d{2}-\d{2}'):
    list_files = [x for x in os.listdir(file_path) if re.search(search_string, x)]
    num_files = len(list_files)
    print("{} files found".format(num_files))

    # Find the most recent file
    most_recent_file = max(list_files)
    most_recent_path = "{}/{}".format(file_path, most_recent_file)
    return most_recent_path


# Helper function that adds a date field
def add_date_field(df_orig, today, now_fmt, operation = 'Run'):
    df = df_orig.copy()

    # Sort by location
    df.sort_values(by=['Location'], inplace=True)
    
    # Insert time stamp
    
    df.insert(0, "Date " + operation, [today] * df.shape[0], True)
    df.insert(1, "Date/Time " + operation, [now_fmt + 'America/Los Angeles TZ'] * df.shape[0], True)
    
    return df



# Read the Dropbox token
dropbox_token = get_dropbox_token()

latest_file_indiv = get_recent_file(file_path = 'output/csv', search_string='^covid_disparities_output_\d{4}-\d{2}-\d{2}')
latest_file_comb = get_recent_file(file_path = 'output/master-table', search_string = '^combinedData\d{4}-\d{2}-\d{2}')

# Date/time stamp
today = datetime.date.today()
now = datetime.datetime.now()
now_fmt = now.strftime("%Y/%m/%d %H:%M:%S")

# Read in the file and add the date/time stamp
df_indiv = add_date_field(pd.read_csv(latest_file_indiv), today, now_fmt)
df_comb = add_date_field(pd.read_csv(latest_file_comb), today, now_fmt, operation='Combined')

# Save the file to CSV
df_indiv.to_csv(latest_file_indiv, index=False)
df_comb.to_csv(latest_file_comb, index=False)

# Make a copy of the file with a static file name that doesn't change from run to run
# Makes it easier to pull the latest dataset into Tableau
copyfile(latest_file_indiv, 'output/latest-single-day-output.csv')
copyfile(latest_file_comb, 'output/latest-combined-output.csv')


# Path to scraper log
scraper_log_path = 'run_scrapers.log'

## Adjust formatting of timestamp to facilitate filenames on different platforms
now_fmt = now.strftime("%Y-%m-%d -- %H-%M-%S")

######################### Upload the files to Dropbox


#### Log file
dropbox_d4bl_upload(scraper_log_path, 'd4bl_logs', now_fmt)

### Single-day output
dropbox_d4bl_upload(latest_file_indiv, 'd4bl_covid19', now_fmt)

### Multi-day output
dropbox_d4bl_upload(latest_file_comb, 'd4bl_combined_output', now_fmt)

### Latest single-day output (copy of single-day file above, but with static file name)
dropbox_d4bl_upload('output/latest-single-day-output.csv', 'd4bl_latest', '')

### Lateat combined output (static file name)
dropbox_d4bl_upload('output/latest-combined-output.csv', 'd4bl_latest', '')

print('Success!')


