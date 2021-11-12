#!/usr/bin/python

import glob
import json
import os
import requests
import pathlib

# Update ES with geo codes
headers = {"Accept": "application/json", "Content-type": "application/json"}
headers_bulk = {"Accept": "application/json", "Content-type": "application/x-ndjson"}

ELASTIC_URL = os.getenv("WM_ELASTIC_URL", "http://10.65.18.69:9200")
ELASTIC_GADM_INDEX = "gadm-name"

#update the metadata one doc at a time
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

def bulk_update(all_items):
    url = ELASTIC_URL + "/" + ELASTIC_GADM_INDEX + "/_bulk"
    body = []
    for item in all_items:
        action = { "update" : {"_id" : item["code"]} }
        item = { "doc" : {"bbox" : item["bbox"]} }
        body.append(json.dumps(action))
        body.append(json.dumps(item))
    payload = ""
    for l in body:
        payload = payload + f"{l} \n"
    data = payload.encode('utf-8')
    res = requests.post(url, data=data, headers=headers_bulk)
    print(res.json())

dir_path = str(pathlib.Path(__file__).parent.resolve()) + '/.tmp/'
fileList = [f for f in glob.glob("*.json")]
print("Found files", fileList)
for file in fileList:
    with open(file) as geojson_file:
        print("Sending bulk update for", file)
        data = json.load(geojson_file)
        # update in bulk
        bulk_update(data["geo"])
        #
        # or update individual docs
        #for region in data["geo"]:
        #    geocode = region["code"]
        #    bbox = region["bbox"]
        #    # update_metadata(geocode, bbox)
        #    print(geocode)

test_data = [
    {
        "code": 'ETH',
        "bbox": {
            "type": "envelope",
            "coordinates": [
                [33.00153732, 14.8454771],
                [47.95822906, 3.39882302]
            ]
        }
    },
    {
        "code": 'ETH.8_1',
        "bbox": {
            "type": "envelope",
            "coordinates": [
                [34.13949966, 10.38686943],
                [42.97984695, 3.50965881]
            ]
        }
    },
    {
        "code": 'EGY',
        "bbox": {
            "type": "envelope",
            "coordinates": [
                [24.69809914, 31.66791725],
                [36.24874878, 21.72538948]
            ]
        }
    }
]

#bulk_update(test_data)
