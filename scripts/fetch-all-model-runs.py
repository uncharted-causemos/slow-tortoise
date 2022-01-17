import json
import sys
import os
import requests
from requests.auth import HTTPBasicAuth

ES_URL = os.getenv("ES_URL", "http://10.65.18.34:9200")
ES_USER = os.getenv("ES_USER", "") # required
ES_PWD = os.getenv("ES_PWD", "") # required

ACCEPTED_STATUS = ["READY", "PROCESSING FAILED", "PROCESSING"]

# Fetch all model run metadata from Causemos. Writes to stdout the runs that match the accepted status and model_id
# Usage: ES_USER=... ES_PWD=... python fetch-all-model-runs.py (optional-model-ids.json) >all-model-runs.json

res = requests.get(f"{ES_URL}/data-model-run/_search?size=10000", auth=HTTPBasicAuth(ES_USER, ES_PWD))
res.raise_for_status()
runs = res.json()["hits"]["hits"]

try:
    with open(sys.argv[1]) as model_ids_file:
        model_ids = json.loads(model_ids_file.read())
    num_model_ids = len(model_ids)
except:
    model_ids = []
    num_model_ids = 0

print(f"Read {num_model_ids} model ids.", file=sys.stderr)

valid_runs = []
excluded_model_names = []
rejected_run_ids = []
status_count = {}
for run_meta in runs:
    run = run_meta["_source"]
    run_id = run["id"]
    model_id = run["model_id"]
    status = run["status"]
    model_name = run["model_name"]

    if status not in status_count:
        status_count[status] = 0
    status_count[status] += 1

    if len(ACCEPTED_STATUS) > 0 and status not in ACCEPTED_STATUS:
        rejected_run_ids.append(run_id)
        continue

    if num_model_ids > 0 and model_id in model_ids:
        valid_runs.append(run)
    else:
        rejected_run_ids.append(run_id)
        excluded_model_names.append(model_name)

print(f"Outputting {len(valid_runs)} runs. Rejected {len(rejected_run_ids)} runs. Rejected {len(excluded_model_names)} models.", file=sys.stderr)
# print(json.dumps(excluded_model_names), file=sys.stderr)
# print(json.dumps(rejected_run_ids), file=sys.stderr)
print(json.dumps(status_count), file=sys.stderr)
json_str = json.dumps(valid_runs, indent=2)
print(json_str)
