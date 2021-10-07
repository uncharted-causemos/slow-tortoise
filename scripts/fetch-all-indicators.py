import requests
import json
from requests.auth import HTTPBasicAuth
import time
import sys
import os

DOJO_USER = os.getenv("DOJO_USER", "")
DOJO_PWD = os.getenv("DOJO_PWD", "")

# Usage: DOJO_USER=... DOJO_PWD=... python fetch-all-indicators.py >all-indicators-08-12.json
all_results = []
scroll_id = None
hits = 1

while len(all_results) < hits:
    if scroll_id is None:
        q_scroll = "?size=1000"
    else:
        q_scroll = "?size=1000&scroll_id=" + scroll_id

    try:
        resp = requests.get(
            "https://dojo-test.com/indicators" + q_scroll,
            auth=HTTPBasicAuth(DOJO_USER, DOJO_PWD),
        )
        resp.raise_for_status()
        ret = resp.json()
    except Exception as exc:
        print(
            f">> Exception: hits {hits}, Len {len(all_results)}, scroll_id {scroll_id}",
            file=sys.stderr,
        )
        print(f"{exc}", file=sys.stderr)
        break

    hits = ret["hits"]
    all_results.extend(ret["results"])
    scroll_id = ret["scroll_id"]

    print(f">> Loop: hits {hits}, Len {len(all_results)}", file=sys.stderr)


json_str = json.dumps(all_results, indent=2)
print(json_str)
