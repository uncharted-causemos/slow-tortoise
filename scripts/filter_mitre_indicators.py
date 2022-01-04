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
    "8077f0b2-c15c-4b0d-af81-91090d82e5f4",
    "eab42bf4-c229-49be-b25e-92387ca2484b",
    "7938a6c4-e106-462b-bdd9-c25de8e246cd",
    "9b42f352-c67b-468d-bcd6-b10a5152e9f9",
    "07cc31fc-09db-41a2-aa91-aed193970a10",
    "bd7f6184-35bd-4c6c-a490-dd2a365dc358",
    "12ecb553-9c50-4f3e-b175-4e3819a2f37b",
]

# Indicators that have been deleted from Causemos
datamart_list = datamart_list + [
    "63f41a7d-ca70-4ccf-ac2f-bda51fb91322",
    "679a577d-c68b-49d4-8c5a-e3337b6755b4",
    "2fb09927-f7a7-48a9-b5bb-fa3807e57f28",
    "edb39596-c68a-43a1-8184-513b1a5b02cb",
    "88e78b3f-c685-4eac-b4d9-27c5e4c22dcf",
    "144458a8-b84f-4177-bd7b-103f06d24931",
    "aa5fe6a4-a991-4538-be26-0a7c9c0ceebf",
    "29ffc038-54cc-40f8-9345-99494ed21903",
]

# Special case indicators
# These are processed and valid indicators in Causemos but they require additional steps to process
datamart_list = datamart_list + [
    # Population datacube registered by Uncharted.
    # To run:
    #  - compute_tiles: false
    #  - Run on dask cluster with nodes that have 32+ GB RAM 
    "430d621b-c4cd-4cb2-8d21-1c590522602c",

    # Large dataset with 100+ features
    # To run:
    #  - compute_tiles: false
    #  - Run on dask cluster with nodes that have 32+ GB RAM 
    "FAOSTAT_EAfrica_crops_primary-PBhattacharya-Aug-21",
    "FAOSTAT_EAfrica_live_animals-PBhattacharya-Aug-21",

    # Large dataset with missing timestamp, should be Jan 2017
    # To run:
    #  - fill_timestamp: 1483228800000
    #  - Run on dask cluster with nodes that have 32+ GB RAM 
    "baaf19ee-e4f0-42da-97a0-21b3340b2839",
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
