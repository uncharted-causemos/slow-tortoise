import sys
import os
import requests
from requests.auth import HTTPBasicAuth

ES_URL = os.getenv("ES_URL", "http://10.65.18.34:9200")
ES_USER = os.getenv("ES_USER", "") # required
ES_PWD = os.getenv("ES_PWD", "") # required

#                                                   WARNING
#                                                   vvvvvvv
#                                                 ___________
# Reads all indicators registered in Causemos and ! DELETES ! duplicates based on data_id and default_feature.
#                                                 -----------
# When duplicated are encountered, the first READY indicator is preserved, and all others are deleted. If none
# are READY, the first PROCESSING indicator is preserved. Indicators with all other statuses are ignored.
# NOTE: You must uncomment the block at the bottom of the file to delete. By default this only dry runs.
# Usage: ES_USER=... ES_PWD=... python dedupe-indicators.py

try:
    resp = requests.get("{ES_URL}/data-datacube/_search?q=type:indicator&size=10000", auth=HTTPBasicAuth(ES_USER, ES_PWD))
    resp.raise_for_status()
    ret = resp.json()
    indicators = ret["hits"]["hits"]
except Exception as exc:
    print(f"{exc}", file=sys.stderr)

unique = {}
dupes = []
for i in indicators:
    ind = i["_source"]
    if ind["type"] != "indicator":
        continue

    data_id = ind["data_id"]
    feature = ind["default_feature"]
    key = f"{data_id}__{feature}"
    ind_id = ind["id"]
    status = ind["status"]

    if key not in unique:
        unique[key] = (ind_id, status)
    else:
        print(
            f"Duplicate {ind_id}: {data_id} // {feature}  {status}\nPreviously seen: {unique[key][0]} with status {unique[key][1]}"
        )
        if unique[key][1] == "READY" or status == "PROCESSING":
            dupes.append((ind_id, status))
        elif unique[key][1] == "PROCESSING" and status == "READY":
            dupes.append(unique[key])
            unique[key] = (ind_id, status)

print("\n==========================================\n")
for key, value in unique.items():
    if value[1] == "PROCESSING":
        print(f"== No data ready for {key}")

for duplicate in dupes:
    print(f"Deleting {duplicate}")
    #
    # UNCOMMENT TO ACTUALLY DELETE
    #
    # if duplicate[1] != 'PROCESSING':
    #     print(f'##### Deleted non-proc {duplicate[0]}')
    # resp = requests.delete(f'{ES_URL}/data-datacube/_doc/{duplicate[0]}', auth=HTTPBasicAuth(ES_USER, ES_PWD))
    # resp.raise_for_status()
    # print(resp.json())
    print("------------------------------------------")
