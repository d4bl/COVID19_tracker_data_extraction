#!/bin/bash

# Script to be executed in Docker image for Data 4 Black Lives COVID-19 Data Tracker
# Author: Sydeaka Watson
# Updated: July 2020
# For detailed instructions, see documentation at the top of the companion Dockerfile

echo "Set parameters"
GITHUB_KEY=YOUR_GITHUB_KEY
GITHUB_USERNAME=YOUR_USERNAME
USER_EMAIL=YOUR_EMAIL
USER_NAME=FirstName_LastName
USER_CENSUS_API=YOUR_CENSUS_API_KEY
USER_GOOGLE_API=YOUR_GOOGLE_API_KEY

echo "\n*****         Activate the environment"
. $env_name/bin/activate

echo "\n*****         Clone Github repository"
cd COVID19_tracker_data_extraction/workflow/python
git clone https://${GITHUB_USERNAME}:${GITHUB_KEY}@github.com/d4bl/COVID19_tracker_data_extraction.git
git config --global user.email "$USER_EMAIL"
git config --global user.name "$USER_NAME"

echo "\n*****         Run the scraper"
cd /COVID19_tracker_data_extraction/workflow/python
python3 run_scrapers.py --census_api_key $USER_CENSUS_API --google_api_key $USER_GOOGLE_API

echo "\n*****         Create the commit message"
echo "$(date +'%B %d, %Y %r')" > /tmp/tmpfile
timestamp=$(cat /tmp/tmpfile)
commit_msg="Results from $timestamp $TZ TZ run"
echo $commit_msg

echo "\n*****         Push the xlsx, csv, and log files to the repo"
git add *.csv -f
git add *.xlsx -f
git add *.log -f
git commit -m "$commit_msg"
git push

echo "\n*****         Success!"
