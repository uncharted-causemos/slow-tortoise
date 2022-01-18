import json
import os
import requests
from requests.auth import HTTPBasicAuth

ES_URL = os.getenv("ES_URL", "http://10.65.18.34:9200")
ES_USER = os.getenv("ES_USER", "") # required
ES_PWD = os.getenv("ES_PWD", "") # required

# Writes to stdout the id, status, and created_at fields of all models registered in Causemos
# Usage: ES_USER=... ES_PWD=... python get-model-ids.py >model_ids.json

body = {
  "query": {
    "match": {
      "type": "model"
    }
  },
  "_source": [
    "id",
    "name",
    "created_at"
  ]
}
res = requests.post(f"{ES_URL}/data-datacube/_search?size=1000", auth=HTTPBasicAuth(ES_USER, ES_PWD), json=body)
res.raise_for_status()
runs = res.json()["hits"]["hits"]

print(json.dumps([run["_source"] for run in runs], indent=2))