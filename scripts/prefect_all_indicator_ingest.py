import json
import sys
import time
import requests

CAUSEMOS_URL = "http://localhost:3000"

# Usage python prefect_all_indicator_ingest.py non-datamart-indicators.json

total_start_time = time.time()

with open(sys.argv[1]) as indicators_file:
    indicators = json.loads(indicators_file.read())
num_indicators = len(indicators)

print(f">> Read {num_indicators} indicators")
for index, indicator in enumerate(indicators):

    indicator_id = indicator["id"]
    print(
        f">> Progess {index}/{num_indicators}. Started {int((time.time() - total_start_time) / 60)} minutes ago."
    )
    print(f">> Processing {indicator_id}")
    start_time = time.time()
    try:
        r = requests.post(f"{CAUSEMOS_URL}/api/maas/indicators/post-process", json=indicator)
        r.raise_for_status()

        print(f">> Submitted {indicator_id}")
    except Exception as exc:
        print(f">> Error processing {indicator_id}")
        print(exc)
    print(">> Finished in %.2f seconds" % (time.time() - start_time))

print("## All completed in %.2f seconds" % (time.time() - total_start_time))
