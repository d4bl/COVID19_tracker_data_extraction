

## Install miniconda 
# NAME:  Miniconda3
# VER:   4.8.2
# PLAT:  osx-64
# LINES: 569
# MD5:   e947884fafc78860e75e43579fa3c270
# https://docs.conda.io/en/latest/miniconda.html

export project_folder=~/Documents/GitHub/d4bl_covid_tracker

pip install --upgrade pip
pip install pipenv

pip install virtualenv

cd $project_folder
export env_name=covid19_data_test
virtualenv $env_name
source $env_name/bin/activate

## Install Jupyter utils.
## Note: The version number for tornado is extremely important here. 
## I wouldn't recommend changing it.
pip install jupyter
pip install ipykernel
pip install tornado==5.1.1
ipython kernel install --user --name=$envname

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




