#!/usr/bin/python

import json
import os
import requests

# Update ES with geo codes
inputFile = "test.json"
headers = {"Accept": "application/json", "Content-type": "application/json"}
ELASTIC_URL = os.getenv("WM_ELASTIC_URL", "http://10.65.18.69:9200")
ELASTIC_GADM_INDEX = "gadm-name"

def update_metadata(code, bbox):
    url = ELASTIC_URL + "/" + ELASTIC_GADM_INDEX + "/_update_by_query"
    query = json.dumps(
        {
            "script": {
                "source": f"ctx._source.bbox = {bbox}",
                "lang": "painless",
            },
            "query": {"bool": {"must": [{"match": {"code": code}}]}},
        }
    )
    res = requests.post(url, data=query, headers=headers)
    print(res.json())

with open(inputFile) as geojson_file:
    data = json.load(geojson_file)
    for region in data["geo"]:
        geocode = region["code"]
        bbox = region["bbox"]
        # update_metadata(geocode, bbox)
        print(geocode)
            
# test
update_metadata('ETH', [33.00153732, 47.95822906, 3.39882302, 14.8454771])
update_metadata('ETH.8_1', [34.13949966, 42.97984695, 3.50965881, 10.38686943])
update_metadata('SSD', [24.15192986, 35.86994934, 3.48099899, 12.21899986])
update_metadata('EGY', [24.69809914, 36.24874878, 21.72538948, 31.66791725])

