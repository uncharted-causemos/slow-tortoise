#!/usr/bin/python

import requests
import pathlib
import csv

# get the list of countries
# This is a chached version of
# https://data.apps.fao.org/catalog/dataset/code-list-gadm36-global-admin-0
csv_url = f"http://10.64.16.209:4005/gadm/gadm36_0.csv"
req = requests.get(csv_url)
url_content = req.content
this_filename = str(pathlib.Path(__file__).parent.resolve()) + '/.tmp/' + "countries.csv"
csv_file = open(this_filename, 'wb')
csv_file.write(url_content)
csv_file.close()

# create a text file for each country with a name that match the gadm code
dir_path = str(pathlib.Path(__file__).parent.resolve()) + '/.tmp/countries/'
with open(this_filename) as f:
    list_of_rows = [{k: v for k, v in row.items()} for row in csv.DictReader(f, skipinitialspace=True)]
    for row in list_of_rows:
        fname = dir_path + row["gadm36_0"] + '.txt'
        print(fname)
        f = open(fname, 'w')
        f.write(row["name_0"])
        f.close()
