
import os
import json
import sys
import time
import requests
from requests.auth import HTTPBasicAuth

DOJO_USR = os.getenv("DOJO_USR", "")
DOJO_PW = os.getenv("DOJO_PW", "")

DOJO_URL = 'https://dojo-test.com'
CAUSEMOS_URL = 'http://localhost:3000'
ES_URL = 'http://10.65.18.34:9200'


def get_model_run_from_es(run_id, es_url=ES_URL):
  # Fetch existing model run metadata
  try:
    res = requests.get(f'{es_url}/data-model-run/_doc/{run_id}')
    res.raise_for_status()
    run_metadata = res.json()['_source']
  except Exception as exc:
    print(f'>> ES: Error fetching model run metadata for {run_id}')
    raise
  return run_metadata
  
def process_model_run(run_metadata, causemos_url=CAUSEMOS_URL):
  run_id = run_metadata['id']
  # Send request to cuasemos for processing
  try:
    r = requests.post(f'{causemos_url}/api/maas/model-runs/{run_id}/post-process', json=run_metadata)
    r.raise_for_status()
    print(f'>> Causemos: Submitted {run_id}')
  except Exception as exc:
    print(f'>> Causemos: Error processing {run_id}')
    raise

def get_indicator_metadata_from_dojo(indicator_id, dojo_url=DOJO_URL):
  try:
    res = requests.get(f'{dojo_url}/indicators/{indicator_id}', auth=HTTPBasicAuth(DOJO_USR, DOJO_PW))
    res.raise_for_status()
    indicator_metadata = res.json()
  except Exception as exc:
    print(f'>> DOJO: Error fetching indicator metadata for {indicator_id}')
    raise
  return indicator_metadata

def delete_indicator_from_es(indicator_id, es_url=ES_URL):
  # Remove existing indicator metadata in ES
  try:
    payload = {
      "query": { "term": { "data_id": indicator_id } }
    }
    res = requests.post(f'{es_url}/data-datacube/_delete_by_query', json=payload)
    res.raise_for_status()
    result = res.json()
    # explicitly wait for index to be refreshed since delete_by_query doesn't support refresh=wait_for
    time.sleep(2)
    print(f'>> ES: Metadata successfully deleted for indicator, {indicator_id}')
  except Exception as exc:
    print(f'>> ES: Error removing existing indicator metadata for {indicator_id}')
    raise

def reprocess_indicator(indicator_metadata, causemos_url=CAUSEMOS_URL):
  indicator_id = indicator_metadata['id']
  # Send request to cuasemos for processing
  try:
    r = requests.post(f'{causemos_url}/api/maas/indicators/post-process', json=indicator_metadata)
    r.raise_for_status()
    print(f'>> Causemos: Submitted {indicator_id}')
  except Exception as exc:
    print(f'>> Causemos: Error processing {indicator_id}')
    raise