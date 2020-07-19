# COVID19_tracker_data_extraction
Data is often not collected by Black communities when it is needed the most. We have compiled a list of all of the states that have shared data on COVID-19 infections and deaths by race and those who have not. This effort is to extract this data from websites to track disparities COVID-19 deaths and cases for Black people.


# Mac Setup
### Install Pip:
```
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py
```

### Install / Setup Virtualenv:
Note: This is simply a recommended way to keep packages for this repo. You can choose to install this globally if you'd like.

#### Installing and setting up virtualenv:
```
pip install virtualenv
virtualenv d4blcovid19tracker
```

#### Activating virtualenv
```
source d4blcovid19tracker/bin/activate
```
Note: Adding an alias to easily enter into this environment can be helpful.

For example, in your ~/.zshrc or ~/.bashrc:
```
alias enter_d4bl='cd /path/to/COVID19_tracker_data_extraction/workflow/python; source /path/to/d4blcovid19tracker/bin/activate'
```

### Installing Packages:
In the python directory (COVID19_tracker_data_extraction/workflow/python) run:
```
./setup_mac.sh
pip install -r requirements.txt
```

### Setup pre-commit hook:
In the root of the repo (./COVID19_tracker_data_extraction), run:
```
pre-commit install
```
This setups a pre-commit hook to lint and format files added to your local git index on a `git commit.` This will run before the commit takes place, so if there are errors, the commit will not take place.
