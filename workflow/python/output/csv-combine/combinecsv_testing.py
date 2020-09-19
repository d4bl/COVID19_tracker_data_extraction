#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 22 15:32:45 2020

@author: jamieprezioso
"""
import pandas as pd
import os

# covid_disparities_output_2020-08-21.csv
csvPath = '../csv/'

# Get all files in csv dir
allFiles = os.listdir('../csv')
print(allFiles)

for f in allFiles:
    print(f)
    if not ('covid_disparities_output_2020-08' in f):
        allFiles.remove(f)
        print('removed ', f)

for f in allFiles:
    print(f)
    if not ('covid_disparities_output_2020-08' in f):
        allFiles.remove(f)
        print('removed ', f)
        
#ocr = 'covid_disparities_output_OCR_2020-06-10.csv'
#goal = 'covid_disparities_output_2020'
#allFiles = allFiles[0:3]
combined_csv = pd.concat([pd.read_csv(os.path.join(csvPath,f)) for f in allFiles])
#export to csv
combined_csv.to_csv( "combined_csv.csv")

# In[]
import shutil
import glob

csvPath = '../csv-short'


#import csv files from folder
path = csvPath
allFiles = glob.glob(path + "/*.csv")
# allFiles.sort()  # glob lacks reliable ordering, so impose your own if output order matters

with open('someoutputfile.csv', 'wb') as outfile:
    for i, fname in enumerate(allFiles):
        with open(fname, 'rb') as infile:
            if i != 0:
                infile.readline()  # Throw away header on all but first file
            # Block copy rest of file from input to output without parsing
            shutil.copyfileobj(infile, outfile)
            print(fname + " has been imported.")



# In[]
            
import os
import glob
import pandas as pd
os.chdir("/mydir")

# Get csv file names
extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]

#combine all files in the list
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
#export to csv
combined_csv.to_csv( "combined_csv.csv", index=False, encoding='utf-8-sig')


# In[]

f1 = '../csv/covid_disparities_output_2020-08-01.csv'
f2 = '../csv/covid_disparities_output_2020-08-19.csv'
df1 = pd.read_csv(f1)
df1.head()
df2 = pd.read_csv(f2)
df2.head()









