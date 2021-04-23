# for execution time
import time
start_time = time.time()

# Dealing with arguments
import argparse
from pathlib import Path

# create a parser instance using ArgumentParser()
parser = argparse.ArgumentParser()
parser.add_argument("dir_path", type=Path, help="Enter your directory!")
parser.add_argument("-u", "--convtime", action="store_true", dest="conv", default=False, help="Keep the Unix timestamp!")
# Now, we will parse all these args using parse_args() method
# Every arg will be accessed in the following manner ::
# args.name :: --> if its positional
# args.dest :: --> if its optional
args = parser.parse_args()

mypath = args.dir_path
# checking for duplicate files
import os
from subprocess import PIPE, Popen

# listdir class :: for retrieving all the contents of a directory --> files, dirs
from os import listdir
# Import isfile() :: boolean checker evaluated to True if the passed is file.
# join :: for construction paths by concatenation.
from os.path import isfile, join

# list for all files in a directory

files = [item for item in listdir(mypath) if isfile(join(mypath, item))]
# empty dict for checksum and file in a key and value format.
checksums = {}
# empty list for the duplicated checksums
duplicates = []

if len(files) == 0:
    print("Directory is empty!")
else:

    # Iterate over the list of files
    for filename in files:
        # print(filename)
        # Use Popen to call the md5sum utility
        with Popen(["md5sum", str(mypath)+ '/' +filename], stdout=PIPE) as proc:
            # checksum command return list of two elements: value of hash function & file name
            # the following expression will retrieve only the value of hash function
            checksum = proc.stdout.read().split()[0]
            # print(checksum)

            # Append duplicate to a list if the checksum is found
            if checksum in checksums:
                duplicates.append(filename)
                # print(duplicates)
            checksums[checksum] = filename
            # print(checksum)

    # checking for duplicate files and removing them
    if len(duplicates) > 0:
        for i in range(len(duplicates)):
            print(f"Found Duplicates: {duplicates[i]}")
            print("remove duplicate file:.... " + duplicates[i])
            os.remove(str(mypath)+'/'+duplicates[i])
    else:
        print("duplicate list is empty")

    # Transform those Unique JSON-files into DataFrames first
    import json
    from pandas.io.json import json_normalize
    import pandas as pd

    #empty list to save unique file names
    files_list = []

    # method to check if dir is empty
    # def dir_empty(args.dir_path):
    #     return not next(os.scandir(args.dir_path), None)
    #     print('Empty directory!')

    # dir_empty('./Files/')

    scanned_dir = os.scandir(mypath)

    for item in scanned_dir:
        if item.is_file() and '.json' in item.name:
            #print("unique file name : {}".format(item.name))
            files_list.append(item.name)
        # else:
        # print(item.name + ' is not a JSON file!')
    # files_list

    # cleaning those files as Dataframes
    for file in files_list:
        mypath = args.dir_path
        print("unique file name " + file + " in path: " + str(mypath))

        records = [json.loads(line) for line in open(str(mypath)+'/'+file)] # reading json file lines
        df = json_normalize(records) # transform this json file into data frame
        df = df[['a', 'r', 'u', 'cy', 'll', 't', 'hc', 'tz']]  # projecting only specific columns

        # cleaning those columns
        df['web_browser'] = df['a'].str.split(' ', None, expand=True)[0]
        df['operating_sys'] = df['a'].str.split(' ', None, expand=True)[1].str.extract(r'([-\w.]+)', expand=True).fillna('Unknown')
        df[['longitude', 'latitude']] = df['ll'].apply(pd.Series)
        df['from_url'] = df['r'].str.split(r'/', None, expand=True)[2].fillna('direct')
        df['to_url'] = df['u'].str.split(r'/', None, expand=True)[2].fillna('direct')

        df.rename(columns={'cy': 'city', 'tz': 'time_zone', 't': 'time_in', 'hc': 'time_out'}, inplace=True)

        # print(args.conv)
        # optional argument handling
        if not args.conv:
            df['time_in'] = pd.to_datetime(df['time_in'], unit='ns')
            df['time_out'] = pd.to_datetime(df['time_out'], unit='ns')

        df = df[['web_browser', 'operating_sys', 'from_url', 'to_url', 'city', 'longitude', 'latitude', 'time_zone','time_in', 'time_out']]

        df.dropna(axis=0, inplace=True)

        transformed_rows = df.shape[0]
        print("No. of rows transformed: "+str(transformed_rows))

        mypath=str(args.dir_path).split('/data')[0]
        df.to_csv(str(mypath) +'/target/' + file.split('.json')[0] +'-cleaned '+'.csv', index=False)

execution_time = time.time() -start_time
print(f"execution_time: {round(execution_time,2)} ")
