# Combine a folder of CSVs to create a single CSV with time series data
# Jamie Prezioso - September 19, 2020

# Import packages
import os
import glob
import pandas as pd
import shutil
import numpy as np
import datetime

# SET THIS PATH.  Should contain only the CSVs you want to merge
path = '../csv-short'

# Get a list of all file names in path
allFiles = glob.glob(path + "/*.csv")
allFiles.sort() # Necessary step

# Create csvList
csvList = [] # Initiate
for f in allFiles:
    # Get dataframe of current file
    dfCurr = pd.read_csv(f)
    
    # Set Date Run, if not already set, from file name
    if not 'Date Run' in dfCurr.keys():
        dateCurr = f[-14:-4:]
        dfCurr['Date Run'] = dateCurr
        print('Adding Date run on ', dateCurr)
    
    # Sort current df by location
    dfCurrSorted = dfCurr.sort_values(by='Location', ascending=True)
    
    # Check for duplicate records: A location should only 
    # appear once in any given dfCurr
    if any(dfCurrSorted.duplicated(subset=['Location'])):
        print('WARNING Duplicate entry in {}'.format(f))
        print('EXITING verify and delete duplicate row')
        break
    
    csvList.append(dfCurrSorted)

dfCombined = pd.concat(csvList, ignore_index=True)

# Combined csv name
date_object = datetime.datetime.now()
print(date_object)

# Export to csv
dfCombined.to_csv("combinedData{}.csv".format(date_object), index=True)