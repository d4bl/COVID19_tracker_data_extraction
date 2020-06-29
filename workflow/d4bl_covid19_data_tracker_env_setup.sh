

## Install miniconda 
# NAME:  Miniconda3
# VER:   4.8.2
# PLAT:  osx-64
# LINES: 569
# MD5:   e947884fafc78860e75e43579fa3c270
# https://docs.conda.io/en/latest/miniconda.html

export project_folder=~/Documents/GitHub/COVID19_tracker_data_extraction

pip install --upgrade pip
pip install pipenv

pip install virtualenv

cd $project_folder
export env_name=covid19_data_test_003
virtualenv $env_name
source $env_name/bin/activate

pip install --upgrade pip

## Install Jupyter utils.
## Note: The version number for tornado is extremely important here. 
## I wouldn't recommend changing it.
pip install jupyter
pip install ipykernel
pip install tornado==5.1.1
ipython kernel install --user --name=$envname
pip install jupyter_contrib_nbextensions
jupyter contrib nbextension install --user

pip install numpy
pip install pandas

pip install datetime
pip install getpass
pip install importlib
pip install bs4
pip install requests
pip install lxml
pip install xlrd
pip install openpyxl

## needed for case 3: pdf table data extraction
pip install tabula-py
pip install backports-datetime-fromisoformat

#pip install fitz
pip install PyMuPDF
pip install pathlib



## case 4: pdf

pip install pytesseract 
pip install Pillow 
pip install matplotlib


## Install tesseract-ocr utility
## https://github.com/tesseract-ocr/tesseract/wiki#homebrew
##   Follow instructions to install homebrew  @https://brew.sh
brew install tesseract

## Install selenium and  chromedriver
brew cask install chromedriver
pip install selenium

## Install google APIs and oauthlib for Colorodo shared drive
pip install google-api-python-client
pip install google-auth-httplib2
pip install google-auth-oauthlib
pip install oauthlib2

## Install ESRI web service API client
pip install arcgis




