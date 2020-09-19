# Table of contents
- [Project overview](#project-overview)
- [Output](#output)
  * [Fields](#fields)
- [Development](#development)
  * [Set up build environment](#set-up-build-environment)
    + [Install Pip](#install-pip)
    + [Install virtualenv](#install-virtualenv)
    + [Create a virtual environment](#create-a-virtual-environment)
    + [Activate the virtual environment](#activate-the-virtual-environment)
    + [Install prerequisite Python packages](#install-prerequisite-python-packages)
    + [Install platform-specific binaries](#install-platform-specific-binaries)
      - [Mac](#mac)
      - [Linux](#linux)
    + [Setup pre-commit hook](#setup-pre-commit-hook)
  * [Run the scrapers](#run-the-scrapers)
    + [Scraper options](#scraper-options)
    + [Register for API keys](#register-for-api-keys)
    + [Limitations](#limitations)
    + [Implemented scrapers](#implemented-scrapers)
  * [Code layout](#code-layout)
  * [Documentation](#documentation)
  * [Style considerations](#style-considerations)
  * [Process](#process)

# Project overview
Data is often not collected by Black communities when it is needed the most. We have compiled a list of all of the states that have shared data on COVID-19 infections and deaths by race and those who have not. This effort is to extract this data from websites to track disparities COVID-19 deaths and cases for Black people.


The scrapers are written in Python, and call out to binaries for PDF data extraction and OCR.

# Output
The default outputs are date-stamped CSV and XLSX files in the
`output/` subdirectory.

## Fields

| **Feature Name** | **Description** |
|-|-|
| Location | The geographic entity for which this row provides data. These can be states, counties, or cities. |
| Date published | The date as of which the underlying data was published by the reporting entity. |
| Date/time of data pull | The date/time the D4BL team ran the code to retrieve the data was retrie. |
| Total Cases | The number of confirmed COVID-19 cases reported for the location. |
| Total Deaths | The number of  deaths attributed to COVID-19 reported for the location. |
| Count Cases Black/AA | The number of confirmed COVID-19 cases corresponding to “Black or African American” or “Non-Hispanic Black” reported for the location. |
| Count Deaths Black/AA | The number of confirmed COVID-19 deaths corresponding to “Black or African American” or “Non-Hispanic Black” reported for the location. |
| Percentage of Cases Black/AA | The percentage of COVID-19 cases (of those with race reported) corresponding to “Black or African American” or “Non-Hispanic Black”. |
| Percentage of Deaths Black/AA | The percentage of COVID-19 deaths (of those with race reported) corresponding to “Black or African American” or “Non-Hispanic Black” |
| Percentage includes unknown race? | Logical (True/False) indicator of whether the `Percentage of Cases Black/AA` field includes COVID-19 cases with race/ethnicity unknown |
| Percentage includes Hispanic Black? | Logical (True/False) indicator of whether the `Percentage of Deaths Black/AA` field includes COVID-19 deaths with race/ethnicity unknown |
| Count Cases Known Race | The number of cases in which race was reported and, hence, “known” |
| Count Deaths Known Race | The number of deaths in which race was reported and, hence, “known” |
| Percentage of Black/AA population (Census data) | The percentage of “Black or African American alone” individuals for the region, computed using 2013-2018 American Community Survey fields [B02001\_003E](https://api.census.gov/data/2018/acs/acs5/variables/B02001_003E.html) and [B02001\_001E](https://api.census.gov/data/2018/acs/acs5/variables/B02001_001E.html). |

**Note:** older output files may not include all of the fields.

# Development

## Set up build environment
Ensure Python 3.8 is installed.

Fork and cloning the repository.  Then change directory to the root of
the repo (`./COVID19\_tracker\_data\_extraction`).  The subsequent
steps need to be run from there.

### Install Pip
If you do not already have `pip`, install it:

```
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py
```

### Install virtualenv
**Note**: This is a recommended way to keep packages for this
repo. You can choose to use a different environment manager such as
`conda`, or even install this globally if you prefer.

```
pip install virtualenv
```

### Create a virtual environment
For example,

```
virtualenv d4blcovid19tracker
```

### Activate the virtual environment
```
source d4blcovid19tracker/bin/activate
```

Adding an alias to easily enter into this environment can be
helpful. For example, in your `~/.zshrc` or `~/.bashrc`:

```
enter_d4bl() {
    cd /path/to/COVID19_tracker_data_extraction/workflow/python
	source /path/to/d4blcovid19tracker/bin/activate
}
```
### Install prerequisite Python packages
```
pip install -r requirements.txt
```

### Install platform-specific binaries
#### Mac
We provide a script wrapping `brew` to install the required non-Python
binaries on Macs.

```
./setup_mac.sh
```

#### Linux
For Linux distributions that use `apt` and `snap`, you can install the
prereqs with these commands:

```
apt install tesseract-ocr
apt install chromium-browser
apt install chromium-chromedriver
snap install chromium
apt install openssl
```

### Setup pre-commit hook
We use `pre-commit` to lint and format files added to your local git
index on a `git commit.` This will run before the commit takes place,
so if there are errors, the commit will not take place.

```
pre-commit install
```

## Run the scrapers
From the `workflow/python` subdirectory, the main script is
`run_scraper.py`.

### Scraper options
There are quite a few options:
```
$ python run_scrapers.py --help
usage: run_scrapers.py [-h] [--list_scrapers] [--work_dir DIR] [--output FILE] [--log_file FILE] [--log_level LEVEL] [--no_log_to_stderr]
                       [--stderr_log_level {CRITICAL,ERROR,WARNING,INFO,DEBUG}] [--google_api_key KEY] [--github_access_token KEY] [--census_api_key KEY]
                       [--enable_beta_scrapers] [--start_date START_DATE] [--end_date END_DATE]
                       [SCRAPER [SCRAPER ...]]

Run some or all scrapers

positional arguments:
  SCRAPER               List of scrapers to run, or all if omitted

optional arguments:
  -h, --help            show this help message and exit
  --list_scrapers       List the known scraper names
  --work_dir DIR        Write working outputs to subdirectories of DIR.
  --output FILE         Write output to FILE (must be -, or have csv or xlsx extension)
  --log_file FILE       Write logs to FILE
  --log_level LEVEL     Set log level for the log_file to LEVEL
  --no_log_to_stderr    Disable logging to stderr.
  --stderr_log_level {CRITICAL,ERROR,WARNING,INFO,DEBUG}
                        Set log level for stderr to LEVEL
  --google_api_key KEY  Provide a key for accessing Google APIs.
  --github_access_token KEY
                        Provide a token for accessing Github APIs.
  --census_api_key KEY  Provide a key for accessing Census APIs.
  --enable_beta_scrapers
                        Include beta scrapers when not specifying scrapers manually.
  --start_date START_DATE
                        If set, acquire data starting on the specified date in ISO format.
  --end_date END_DATE   If set, acquire data through the specified date in ISO format, inclusive.
```

### Register for API keys
Depending on the scrapers invoked, you need to provide keys.  Here are
links on how to register for them:

* [Google API key](https://developers.google.com/drive/api/v3/quickstart/python): Required for `Colorado`.
* [Github access token](https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token): Required for `NewYorkCity`.
* [Census API key](https://api.census.gov/data/key_signup.html): Recommended for all scrapers.

### Limitations
There are no beta scrapers at this time, and the date range options
are not broadly implemented yet.

### Implemented scrapers
The currently implemented scrapers are:

```
$ python run_scrapers.py --list_scrapers
Known scrapers:
  Alabama
  Alaska
  Arizona
  Arkansas
  California
  CaliforniaLosAngeles
  CaliforniaSanDiego
  CaliforniaSanFrancisco
  Colorado
  Connecticut
  Delaware
  Florida
  FloridaMiamiDade
  FloridaOrange
  Georgia
  Hawaii
  Idaho
  Illinois
  Indiana
  Iowa
  Kansas
  Kentucky
  Louisiana
  Maine
  Maryland
  Massachusetts
  Michigan
  Minnesota
  Mississippi
  Missouri
  Montana
  Nebraska
  Nevada
  NewHampshire
  NewMexico
  NewYork
  NewYorkCity
  NorthCarolina
  NorthDakota
  Ohio
  Oklahoma
  Oregon
  Pennsylvania
  RhodeIsland
  SouthCarolina
  SouthDakota
  Tennessee
  Texas
  TexasBexar
  Utah
  Vermont
  Virginia
  Washington
  WashingtonDC
  WestVirginia
  Wisconsin
  WisconsinMilwaukee
  Wyoming
```


## Code layout
## Documentation
## Style considerations
## Process
