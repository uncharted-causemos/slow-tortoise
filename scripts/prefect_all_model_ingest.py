import json
import sys
import time
import common

# Sumbit all model runs from the input file for post-processing.
# The input file can be created with `fetch-all-model-runs.py`
# Usage: python prefect_all_model_ingest.py model-runs.json

total_start_time = time.time()

with open(sys.argv[1]) as model_runs_file:
    runs = json.loads(model_runs_file.read())
num_runs = len(runs)

print(f">> Read {num_runs} model runs")
for index, run_json in enumerate(runs):
    common.process_model_run(run_json)
print("## All completed in %.2f seconds" % (time.time() - total_start_time))
