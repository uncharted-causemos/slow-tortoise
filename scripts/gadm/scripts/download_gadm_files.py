from scripts.gadm.scripts.common_gadm import MAX_GADM_INDEX, get_csv_filename
import requests
import os

# Run scripts in this order
# 1. download_gadm_files.py (this script)
# 2. populate_with_region_names.py

def download_gadm():
    for index in range(MAX_GADM_INDEX):
        csv_url = f"http://10.64.16.209:4005/gadm/gadm36_{index}.csv"
        req = requests.get(csv_url)
        url_content = req.content
        csv_file = open(get_csv_filename(index), 'wb')
        csv_file.write(url_content)
        csv_file.close()