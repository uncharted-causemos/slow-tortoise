import requests
import json
import time
import sys

try:
    resp = requests.get("http://10.65.18.34:9200/data-datacube/_search?q=type:indicator&size=5000")
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
    # resp = requests.delete(f'http://10.65.18.34:9200/data-datacube/_doc/{duplicate[0]}')
    # resp.raise_for_status()
    # print(resp.json())
    print("------------------------------------------")
