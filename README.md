# Python-Scripting-Task-JSON-Cleaning-to-CSV
Automated (using crontab) Python script to handle duplicate JSON files in directory and then loads them into dataframes, then do some cleaning, and finally convert these dataframes into CSV files

script can handle if directory is empty as well

used library like: time, subprocess , pandas, pathlib, os, json, json_normalize

#Don't forget to change the pathes inside the Schedular.sh and inside you crontab on your linux system
#also the path for your JSON file's directory and the target directory in which you want to put you final CSV files
