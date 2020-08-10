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


# Helper function to get the Dropbox token
def get_dropbox_token():
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




# Read the Dropbox token
dropbox_token = get_dropbox_token()

# Find the most recent CSV file
#csv_path = '/COVID19_tracker_data_extraction/workflow/python/output/csv'
csv_path = 'output/csv'
csv_files = [x for x in os.listdir(csv_path) if re.search('^covid_disparities_output_\d{4}-\d{2}-\d{2}',x)]
num_files = len(csv_files)
print("{} files found".format(num_files))

# Find the most recent file
csv_most_recent_file = max(csv_files); csv_most_recent_file
csv_most_recent_path = "{}/{}".format(csv_path, csv_most_recent_file)

# Read in the file
df = pd.read_csv(csv_most_recent_path)

# Sort by location
df.sort_values(by=['Location'], inplace=True)

# Insert time stamp
today = datetime.date.today()
now = datetime.datetime.now()
now_fmt = now.strftime("%Y/%m/%d %H:%M:%S")
df.insert(0, "Date Run", [today] * df.shape[0], True)
df.insert(1, "Date/Time Run", [now_fmt + 'America/Los Angeles TZ'] * df.shape[0], True)

# Save the file
df.to_csv(csv_most_recent_path, index=False)

# Path to scraper log
#scraper_log_path = '/COVID19_tracker_data_extraction/workflow/python/run_scrapers.log'
scraper_log_path = 'run_scrapers.log'

# Upload the file to Dropbox
now_fmt = now.strftime("%Y-%m-%d -- %H-%M-%S")
dropbox_d4bl_upload(csv_most_recent_path, 'd4bl_covid19', now_fmt)
dropbox_d4bl_upload(scraper_log_path, 'd4bl_logs', now_fmt)

print('Success!')


