# Docker script for Data 4 Black Lives COVID-19 Data Tracker
# Author: Sydeaka Watson
# Updated: July 2020
#
# This Dockerfile sets up the environment in which the COVID-19 Data Tracker will run in Google Cloud 
#  or any other computing environment. After building the image, the script `execute.sh` 
#  (provided in the same folder) does the following:
#  - clones the scraper Github repository
#  - runs the scraper for all locations
#  - commits and pushes the scraper output to the repository.
# 
# There are two primary ways to use this Docker implementation.
#
# Option #1: Manually run on a local machine:
# # Build the `covid19tracker` image. Note the period at the end.
#     docker build -t covid19tracker .
# 
# # Execute the shell script
#     <<TO DO: INSERT CODE HERE>>
#
#  
# `execute.sh` currently expects the user to replace arguments with hard-coded values for Github credentials, 
#    a Census API key, and a Google API key. Alternatively, these parameters can be safely removed and 
#    replaced with an external credentials file. To do this, the user would prepare a text file called 
#    "build.args" (in the same folder as the Dockerfile) containing credentials 
#    for each config parameter one per line, as in the following example:
#  
#
#     GITHUB_KEY=YOUR_GITHUB_KEY
#     GITHUB_USERNAME=YOUR_USERNAME
#     USER_EMAIL=YOUR_EMAIL
#     USER_NAME=FirstName LastName
#     USER_CENSUS_API=YOUR_CENSUS_API_KEY
#     USER_GOOGLE_API=YOUR_GOOGLE_API_KEY
#
#
# Then, to build the file using the args file, the user would run the following. 
# Note the period at the end:
#     docker build -t covid19tracker $(for i in `cat build.args`; do out+="--build-arg $i " ; done; echo $out;out="") . 
#
# NOTE: Building the image places a copy of `execute.sh` inside the container that will be available 
#  while the container is running. Thus, any changes to `execute.sh` should be made prior to 
#  building the image.
#
#
# Option #2: Set up a cron job that automatically runs the scraper at pre-specified times.
# Helpful links for running on Google Cloud:
#   - Setting up Google Cloud account: https://console.cloud.google.com
#   - Billing console: https://console.cloud.google.com/billing/
#   - Building a Docker image in Google Cloud: 
# 	- Setting up CronJob: https://cloud.google.com/kubernetes-engine/docs/how-to/cronjobs#using-gcloud-config
#   - Setting job frequency: https://crontab.guru/
#	
#
# When running on some systems (including Ubuntu on Oracle Cloud), I had to set permissions 
#  on `docker.sock` to resolve "permission denied" error prior to running the docker container.
# This was resolved using the following code:
#     sudo chmod 666 /var/run/docker.sock
#
# Reference: https://www.digitalocean.com/community/questions/how-to-fix-docker-got-permission-denied-while-trying-to-connect-to-the-docker-daemon-socket
#
#
#
# 

# Using Ubuntu version 19.10
FROM ubuntu:19.10

# Update apt-get repository
RUN apt-get update -y && \
apt-get install wget -y

# Create setup folder
RUN echo "\n*****         Create setup folder"
RUN mkdir setup && \
cd setup

# Set up Miniconda
RUN echo "\n*****         Set up Miniconda"
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
chmod 777 Miniconda3-latest-Linux-x86_64.sh && \
yes yes | ./Miniconda3-latest-Linux-x86_64.sh && \
cd ..

# Install pip3
RUN echo "\n*****         Install pip3"
RUN apt update -y && \
yes yes | apt install python3-pip 

# Install openssl
RUN apt-get install openssl

# Install Google Chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
RUN apt-get -y update
RUN apt-get install -y google-chrome-stable

# Install chromedriver
RUN apt-get install -yqq unzip
RUN yes yes | apt install curl
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN yes yes | unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/

# Install virtual environment utils
RUN echo "\n*****         Install virtual environment utils"
RUN pip3 install --upgrade pip && \
pip3 install pipenv && \
apt install python3-virtualenv -y && \
pip3 install virtualenvwrapper && \
apt-get install python3-venv -y

# Create the virtual environment
RUN echo "\n*****         Create the virtual environment"
ENV project_folder /COVID19_tracker_data_extraction/workflow/python
ENV env_name covid19_data_test_003
RUN python3 -m venv $env_name
#RUN source $env_name/bin/activate
RUN . $env_name/bin/activate && \
pip3 install selenium 

# Install tesseract
RUN echo "\n*****         Install tesseract"
RUN yes yes | apt-get update && \
echo "12" | apt-get install tesseract-ocr -y

# Install git
RUN echo "\n*****         Install git"
RUN yes yes | apt install git

# Install Jupyter utils
RUN . $env_name/bin/activate && \
echo "\n*****         Install Jupyter utils." && \
# Note: The version number for tornado is extremely important here. 
# I wouldn't recommend changing it.
pip3 install jupyter && \
pip3 install ipykernel && \
pip3 install tornado==5.1.1 && \
ipython kernel install --user --name=$envname && \
pip3 install jupyter_contrib_nbextensions && \
jupyter contrib nbextension install --user

# Install misc packages
RUN . $env_name/bin/activate && \
echo "\n*****         Install misc packages" && \
pip3 install numpy && \
pip3 install pandas && \
pip3 install datetime && \
pip3 install importlib && \
pip3 install bs4 && \
pip3 install requests && \
pip3 install lxml && \
pip3 install xlrd && \
pip3 install openpyxl && \
pip3 install pathlib

# Install packages for PDF data extraction
RUN . $env_name/bin/activate && \
echo "\n*****         Install packages for PDF data extraction" && \
pip3 install tabula-py && \
pip3 install backports-datetime-fromisoformat && \
apt-get update -y && \
apt-get install mupdf mupdf-tools -y

# Install OCR pdf utils
RUN . $env_name/bin/activate && \
echo "\n*****         Install OCR pdf utils" && \
pip3 install pytesseract && \
pip3 install Pillow && \
pip3 install matplotlib

# Install google APIs and oauthlib for Colorodo shared drive
RUN . $env_name/bin/activate && \
echo "\n*****         Install google APIs and oauthlib for Colorodo shared drive" && \
pip3 install google-api-python-client && \
pip3 install google-auth-httplib2 && \
pip3 install google-auth-oauthlib

# Install ESRI web service API client
RUN . $env_name/bin/activate && \
echo "\n*****         Install ESRI web service API client" && \
pip3 install arcgis

# Install Java
RUN . $env_name/bin/activate && \
echo "\n*****         Install Java" && \
yes yes | apt update && \
yes yes | apt install default-jdk

# Selenium-wire install
RUN . $env_name/bin/activate && \
pip install selenium-wire

# Install PyGithub for New York City
RUN . $env_name/bin/activate && \pip install PyGithub

# Install openssl
RUN . $env_name/bin/activate && \
pip3 install wheel && \
apt-get install openssl && \
pip install pydash

# Install PDF utils
RUN . $env_name/bin/activate && \
pip install --upgrade pip && \
pip install -U PyMuPDF

# Set time zone to Central (i.e., America/Chicago)
ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Chicago
RUN apt-get install -y tzdata

# Current timestamp in selected time zone
RUN echo "Current timestamp in selected time zone:" && \
echo $(date +'%B %d, %Y %r') && \
echo $TZ

# Clean up
RUN apt-get -qy autoremove

# Set PORT
ENV PORT 8080

# Import a copy of the execute.sh script that runs the scraper 
COPY execute.sh /
RUN chmod 777 execute.sh




