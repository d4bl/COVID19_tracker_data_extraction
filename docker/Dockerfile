# Docker script for Data 4 Black Lives COVID-19 Data Tracker
# Author: Sydeaka Watson
#
# Prepare a text file called "build.args" (in the same folder as the Dockerfile) containing credentials 
#  for each config parameter one per line, as in the following example:
#  
#
#     GITHUB_KEY=YOUR_GITHUB_KEY
#     GITHUB_USERNAME=YOUR_USERNAME
#     USER_EMAIL=YOUR_EMAIL
#     USER_NAME=FirstName LastName
#     USER_CENSUS_API=YOUR_CENSUS_API_KEY
#     USER_GOOGLE_API=YOUR_GOOGLE_API_KEY
#
# To build the file using the args file, run:
#     docker build -t tracker $(for i in `cat build.args`; do out+="--build-arg $i " ; done; echo $out;out="") . --build-arg kick=0
#
# When running on some systems (including Ubuntu on Oracle Cloud), I had to set permissions on docker.sock to resolve "permission denied" error
#   prior to running the docker container.
# Reference: https://www.digitalocean.com/community/questions/how-to-fix-docker-got-permission-denied-while-trying-to-connect-to-the-docker-daemon-socket
#   sudo chmod 666 /var/run/docker.sock
#
#
# 

# Using Ubuntu version 19.10
FROM ubuntu:19.10

RUN apt-get update -y && \
apt-get install wget -y

RUN echo "\n*****         Create setup folder"
RUN mkdir setup && \
cd setup

RUN echo "\n*****         Set up Miniconda"
RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh && \
chmod 777 Miniconda3-latest-Linux-x86_64.sh && \
yes yes | ./Miniconda3-latest-Linux-x86_64.sh && \
cd ..

RUN echo "\n*****         Install pip3"
RUN apt update -y && \
yes yes | apt install python3-pip 

RUN apt-get install openssl

# install google chrome
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
RUN apt-get -y update
RUN apt-get install -y google-chrome-stable

# install chromedriver
RUN apt-get install -yqq unzip
RUN yes yes | apt install curl
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN yes yes | unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/

RUN echo "\n*****         Install virtual environment utils"
RUN pip3 install --upgrade pip && \
pip3 install pipenv && \
apt install python3-virtualenv -y && \
pip3 install virtualenvwrapper && \
apt-get install python3-venv -y

RUN echo "\n*****         Create the virtual environment"
ENV project_folder /COVID19_tracker_data_extraction/workflow/python
ENV env_name covid19_data_test_003
RUN python3 -m venv $env_name
#RUN source $env_name/bin/activate
RUN . $env_name/bin/activate && \
pip3 install selenium 

RUN echo "\n*****         Install tesseract"
RUN yes yes | apt-get update && \
echo "12" | apt-get install tesseract-ocr -y

RUN echo "\n*****         Install git"
RUN yes yes | apt install git

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
pip3 install openpyxl

RUN . $env_name/bin/activate && \
echo "\n*****         Install packages for PDF data extraction" && \
pip3 install tabula-py && \
pip3 install backports-datetime-fromisoformat

RUN . $env_name/bin/activate && \
pip3 install pathlib

RUN . $env_name/bin/activate && \
apt-get update -y && \
apt-get install mupdf mupdf-tools -y

RUN . $env_name/bin/activate && \
echo "\n*****         Install OCR pdf utils" && \
pip3 install pytesseract && \
pip3 install Pillow && \
pip3 install matplotlib

RUN . $env_name/bin/activate && \
echo "\n*****         Install google APIs and oauthlib for Colorodo shared drive" && \
pip3 install google-api-python-client && \
pip3 install google-auth-httplib2 && \
pip3 install google-auth-oauthlib

RUN . $env_name/bin/activate && \
echo "\n*****         Install ESRI web service API client" && \
pip3 install arcgis

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

# cleanup
RUN apt-get -qy autoremove

# Build arguments
ARG GITHUB_KEY
ARG GITHUB_USERNAME
ARG USER_EMAIL
ARG USER_NAME
ARG USER_CENSUS_API
ARG USER_GOOGLE_API

RUN echo "\n*****         Clone Github repository"
RUN git clone https://${GITHUB_USERNAME}:${GITHUB_KEY}@github.com/d4bl/COVID19_tracker_data_extraction.git

# Set time zone to Central (i.e., America/Chicago)
ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Chicago
RUN apt-get install -y tzdata

RUN echo "Current timestamp in selected time zone:" && \
echo $(date +'%B %d, %Y %r') && \
echo $TZ

# Pull latest code from the repo
ARG kick=10
RUN cd /COVID19_tracker_data_extraction/workflow/python && \
git config --global user.email "$USER_EMAIL" && \
git config --global user.name "$USER_NAME" && \
git reset --hard && \
git pull 

RUN echo "\n*****         Run the scraper"
RUN echo "$(which python)" >> /tmp/tmpfile && \
echo $(cat /tmp/tmpfile)

RUN . $env_name/bin/activate && \
cd /COVID19_tracker_data_extraction/workflow/python && \
python3 run_scrapers.py --census_api_key $USER_CENSUS_API --google_api_key $USER_GOOGLE_API

RUN echo "$(date +'%B %d, %Y %r')" > /tmp/tmpfile
RUN timestamp=$(cat /tmp/tmpfile) && \
commit_msg="Results from $timestamp $TZ TZ run" && \
echo $commit_msg && \
cd /COVID19_tracker_data_extraction/workflow/python && \ 
git pull && \
git add *.csv -f && \
git add *.xlsx -f && \
git add *.log -f && \
git commit -m "$commit_msg"

RUN cd /COVID19_tracker_data_extraction/workflow/python && \
git push

