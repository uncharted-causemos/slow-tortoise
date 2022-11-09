import os
import json
import time
import requests
from requests.auth import HTTPBasicAuth

DOJO_URL = os.getenv("DOJO_URL", "https://dojo-test.com")
DOJO_USER = os.getenv("DOJO_USER", "")
DOJO_PWD = os.getenv("DOJO_PWD", "")

ES_URL = os.getenv("ES_URL", "http://localhost:9200")
ES_USER = os.getenv("ES_USER", "") # required
ES_PWD = os.getenv("ES_PWD", "") # required

CAUSEMOS_URL = os.getenv("CAUSEMOS_URL", "http://localhost:3000")
CAUSEMOS_USER = os.getenv("CAUSEMOS_USER", "")
CAUSEMOS_PWD = os.getenv("CAUSEMOS_PWD", "")


def get_model_run_from_es(run_id, es_url=ES_URL):
    # Fetch existing model run metadata
    auth = None
    if ES_USER != "":
        auth=HTTPBasicAuth(ES_USER, ES_PWD)
    try:
        res = requests.get(f"{es_url}/data-model-run/_doc/{run_id}", auth=auth)
        res.raise_for_status()
        run_metadata = res.json()["_source"]
    except Exception as exc:
        print(f">> ES: Error fetching model run metadata for {run_id}")
        raise
    return run_metadata

def get_model_run_from_dojo(run_id, dojo_url=DOJO_URL):
    try:
        res = requests.get(
            f"{dojo_url}/runs/{run_id}", auth=HTTPBasicAuth(DOJO_USER, DOJO_PWD)
        )
        res.raise_for_status()
        indicator_metadata = res.json()
    except Exception as exc:
        print(f">> DOJO: Error fetching model run metadata for {run_id}")
        raise
    return indicator_metadata

def process_model_run(run_metadata, causemos_url=CAUSEMOS_URL):
    run_id = run_metadata["id"]
    auth = None
    if CAUSEMOS_USER != "":
        auth=HTTPBasicAuth(CAUSEMOS_USER, CAUSEMOS_PWD)
    # Send request to cuasemos for processing
    try:
        r = requests.post(
            f"{causemos_url}/api/maas/model-runs/{run_id}/post-process", auth=auth, json=run_metadata
        )
        r.raise_for_status()
        print(f">> Causemos: Submitted {run_id}")
    except Exception as exc:
        print(f">> Causemos: Error processing {run_id}")
        raise


def get_id_list_from_es_response(es_response):
    ids = []
    for model in es_response["hits"]["hits"]:
        id = model["_source"]["id"]
        ids.append(id)

    json_str = json.dumps(ids, indent=2)
    return json_str


def get_indicator_metadata_from_dojo(indicator_id, dojo_url=DOJO_URL):
    try:
        res = requests.get(
            f"{dojo_url}/indicators/{indicator_id}", auth=HTTPBasicAuth(DOJO_USER, DOJO_PWD)
        )
        res.raise_for_status()
        indicator_metadata = res.json()
    except Exception as exc:
        print(f">> DOJO: Error fetching indicator metadata for {indicator_id}")
        raise
    return indicator_metadata


def delete_indicator_from_es(indicator_id, es_url=ES_URL):
    # Remove existing indicator metadata in ES
    try:
        payload = {"query": {"term": {"data_id": indicator_id}}}
        res = requests.post(f"{es_url}/data-datacube/_delete_by_query", auth=HTTPBasicAuth(ES_USER, ES_PWD), json=payload)
        res.raise_for_status()
        result = res.json()
        # explicitly wait for index to be refreshed since delete_by_query doesn't support refresh=wait_for
        time.sleep(2)
        print(f">> ES: Metadata successfully deleted for indicator, {indicator_id}")
    except Exception as exc:
        print(f">> ES: Error removing existing indicator metadata for {indicator_id}")
        raise

def delete_by_query_string_from_es(index, field, query, es_url=ES_URL):
    # Remove documents from ES using query string to allow wildcards (ex. query="UAZ_VUAZ-*")
    try:
        payload = {"query_string": {"fields": [field], "query": query}}
        res = requests.post(f"{es_url}/{index}/_delete_by_query", auth=HTTPBasicAuth(ES_USER, ES_PWD), json=payload)
        res.raise_for_status()
        result = res.json()
        print(result)
    except Exception as exc:
        print(f">> ES: Error removing documents")
        raise

def reprocess_indicator(indicator_metadata, causemos_url=CAUSEMOS_URL):
    indicator_id = indicator_metadata["id"]
    auth = None
    if CAUSEMOS_USER != "":
        auth=HTTPBasicAuth(CAUSEMOS_USER, CAUSEMOS_PWD)
    # Send request to cuasemos for processing
    try:
        print(auth)
        r = requests.post(
            f"{causemos_url}/api/maas/indicators/post-process", auth=auth, json=indicator_metadata
        )
        r.raise_for_status()
        print(f">> Causemos: Submitted {indicator_id}")
    except Exception as exc:
        print(f">> Causemos: Error processing {indicator_id}")
        raise
