import json
import sys
import os
import requests
from requests.auth import HTTPBasicAuth

ES_URL = os.getenv("ES_URL", "http://10.65.18.34:9200")
ES_USER = os.getenv("ES_USER", "") # required
ES_PWD = os.getenv("ES_PWD", "") # required

# Writes to stdout the data_ids of all datacubes mentioned in the indicator-match-history index
# ie. All data_ids that have been used for grounding
# Usage: ES_USER=... ES_PWD=... python get-model-ids.py >data_ids.json

res = requests.get(f"{ES_URL}/indicator-match-history/_search?size=10000", auth=HTTPBasicAuth(ES_USER, ES_PWD))
res.raise_for_status()
match_history = res.json()["hits"]["hits"]

indicator_ids = set([match_doc["_source"]["indicator_id"] for match_doc in match_history])

data_ids = set()
deleted_ids = set()
for indicator_id in indicator_ids:
    try:
      res = requests.get(f"{ES_URL}/data-datacube/_doc/{indicator_id}", auth=HTTPBasicAuth(ES_USER, ES_PWD))
      res.raise_for_status()
      data_id = res.json()["_source"]["data_id"]

      if data_id.startswith("UAZ"):
          data_ids.add(data_id)
    except:
      deleted_ids.add(data_id)

print(f"Found {len(data_ids)} data_ids. Unable to find {len(deleted_ids)} ids", file=sys.stderr)

json_str = json.dumps(list(data_ids), indent=2)
print(json_str)
