import json
import sys
import time
import requests

CAUSEMOS_URL = "http://localhost:3000"

# Usage python filter_mitre_indicators.py all-indicators-once.json datamart.txt >output.json
total_start_time = time.time()

with open(sys.argv[1]) as indicators_file:
    indicators = json.loads(indicators_file.read())
num_indicators = len(indicators)

with open(sys.argv[2]) as datamart_file:
    lines = datamart_file.readlines()
    datamart_list = [did.rstrip() for did in lines]
num_datamart = len(datamart_list)

# Old indicators that should have been deleted from Dojo
datamart_list = datamart_list + [
    "bd7f6184-35bd-4c6c-a490-dd2a365dc358",
    "12ecb553-9c50-4f3e-b175-4e3819a2f37b",
]

non_datamart = []
print(f">> Read {num_indicators} indicators, {num_datamart} datamart IDs.", file=sys.stderr)
for indicator in indicators:
    indicator_id = indicator["id"]
    if indicator_id not in datamart_list:
        non_datamart.append(indicator)

print(f"Outputting {len(non_datamart)} indicators", file=sys.stderr)
print(f"Skipped {num_indicators - len(non_datamart)} indicators.", file=sys.stderr)
json_str = json.dumps(non_datamart, indent=2)
print(json_str)
