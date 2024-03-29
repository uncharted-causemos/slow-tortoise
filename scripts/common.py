import uuid
import time
import requests
from requests.auth import HTTPBasicAuth
from typing import TypedDict
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan, bulk

ES_INDEX_DATACUBE = "data-datacube"
ES_INDEX_MODEL_RUN = "data-model-run"
ES_INDEX_DOMAIN_PROJECT = "domain-project"
ES_INDEX_PROJECT = "project"
ES_INDEX_ANALYSIS = "analysis"
ES_INDEX_INSIGHT = "insight"
ES_INDEX_QUESTION = "question"


class CausemosApiConfig(TypedDict):
    """
    Represents the configuration for the Causemos API.

    Attributes:
    - url (str): The URL of the Causemos API.
    - user (str): The username for authentication.
    - pwd (str): The password for authentication.
    """

    url: str
    user: str
    pwd: str


class DojoApiConfig(TypedDict):
    """
    Represents the configuration for the Dojo API.

    Attributes:
    - url (str): The URL of the Dojo API.
    - user (str): The username for authentication.
    - pwd (str): The password for authentication.
    """

    url: str
    user: str
    pwd: str


class ESConnectionConfig(TypedDict):
    """
    Represents the configuration for an Elasticsearch connection.

    Attributes:
    - url (str): The URL of the Elasticsearch cluster.
    - user (str): The username for authentication.
    - pwd (str): The password for authentication.
    """

    url: str
    user: str
    pwd: str


class ESConnectionConfigWithIndex(ESConnectionConfig):
    """
    Represents the configuration for an Elasticsearch connection with an additional index.

    Attributes:
    - url (str): The URL of the Elasticsearch cluster.
    - user (str): The username for authentication.
    - pwd (str): The password for authentication.
    - index (str): The Elasticsearch index to operate on.
    """

    index: str


DEFAULT_LOCAL_CAUSEMOS_API_CONFIG: CausemosApiConfig = {
    "url": "http://localhost:3000",
    "user": "",
    "pwd": "",
}

DEFAULT_LOCAL_ES_CONFIG: ESConnectionConfig = {
    "url": "http://localhost:9200",
    "user": "",
    "pwd": "",
}


def process_model_run(
    run_metadata,
    selected_output_tasks=[],
    causemos_api_config=DEFAULT_LOCAL_CAUSEMOS_API_CONFIG,
):
    """
    Submit a data pipeline processing job for a given model run.

    Note:
    - The model run document with the same run ID as `run_metadata` should already exist in the target Causemos system (in Elasticsearch).
    - This operation does not create a new model run document but updates the existing one with the provided metadata.
    """
    # remove flow run metadata properties if already exists
    run_metadata.pop("flow_id", None)
    run_metadata.pop("runtimes", None)
    run_id = run_metadata["id"]

    auth = None
    query = ""
    if causemos_api_config["user"] != "":
        auth = HTTPBasicAuth(causemos_api_config["user"], causemos_api_config["pwd"])
    if len(selected_output_tasks) > 0:
        query = f"?selected_output_tasks={','.join(selected_output_tasks)}"
    # Send request to cuasemos for processing
    try:
        r = requests.post(
            f"{causemos_api_config['url']}/api/maas/model-runs/{run_id}/post-process{query}",
            auth=auth,
            json=run_metadata,
        )
        r.raise_for_status()
        print(f">> Causemos: Submitted {run_id}")
    except Exception as exc:
        print(f">> Causemos: Error processing {run_id}")
        raise


def process_indicator(
    indicator_metadata,
    selected_output_tasks=[],
    causemos_api_config=DEFAULT_LOCAL_CAUSEMOS_API_CONFIG,
):
    # remove flow run metadata properties if already exists
    indicator_metadata.pop("flow_id", None)
    indicator_metadata.pop("runtimes", None)

    indicator_id = indicator_metadata["id"]
    auth = None
    query = ""
    if causemos_api_config["user"] != "":
        auth = HTTPBasicAuth(causemos_api_config["user"], causemos_api_config["pwd"])
    if len(selected_output_tasks) > 0:
        query = f"?selected_output_tasks={','.join(selected_output_tasks)}"
    # Send request to cuasemos for processing
    try:
        r = requests.post(
            f"{causemos_api_config['url']}/api/maas/indicators/post-process{query}",
            auth=auth,
            json=indicator_metadata,
        )
        r.raise_for_status()
        print(f">> Causemos: Submitted {indicator_id}")
    except Exception as exc:
        print(f">> Causemos: Error processing {indicator_id}")
        raise


def get_model_run_from_dojo(run_id, config: DojoApiConfig):
    try:
        res = requests.get(
            f"{config['url']}/runs/{run_id}", auth=HTTPBasicAuth(config["user"], config["pwd"])
        )
        res.raise_for_status()
        indicator_metadata = res.json()
    except Exception as exc:
        print(f">> DOJO: Error fetching model run metadata for {run_id}")
        raise
    return indicator_metadata


def get_indicator_metadata_from_dojo(indicator_id, config: DojoApiConfig):
    try:
        res = requests.get(
            f"{config['url']}/indicators/{indicator_id}",
            auth=HTTPBasicAuth(config["user"], config["pwd"]),
        )
        res.raise_for_status()
        indicator_metadata = res.json()
    except Exception as exc:
        print(f">> DOJO: Error fetching indicator metadata for {indicator_id}")
        raise
    return indicator_metadata


# =================== ES helper functions ================


def create_es_client(config=DEFAULT_LOCAL_ES_CONFIG):
    client = Elasticsearch([config["url"]], http_auth=(config["user"], config["pwd"]))
    return client


def get_model_run_from_es(run_id, config=DEFAULT_LOCAL_ES_CONFIG):
    client = create_es_client(config)
    document = client.get(index=ES_INDEX_MODEL_RUN, id=run_id)
    data = document["_source"]
    return data


def get_data_ids_from_es(status="READY", type="indicator", config=DEFAULT_LOCAL_ES_CONFIG):
    client = create_es_client(config)
    query = {
        "query": {"bool": {"filter": [{"term": {"status": status}}, {"term": {"type": type}}]}},
        "_source": ["data_id"],
    }
    ids = set()
    for hit in scan(client, index=ES_INDEX_DATACUBE, query=query):
        ids.add(hit["_source"]["data_id"])
    return list(ids)


def get_model_run_ids_from_es(
    status="READY", config=DEFAULT_LOCAL_ES_CONFIG, prefix_model_id=False
):
    client = create_es_client(config)
    query = {
        "query": {"bool": {"filter": [{"term": {"status": status}}]}},
        "_source": ["id", "model_id"],
    }
    ids = set()
    for hit in scan(client, index=ES_INDEX_MODEL_RUN, query=query):
        id = hit["_source"]["id"]
        if prefix_model_id:
            id = f'{hit["_source"]["model_id"]}:{id}'
        ids.add(id)
    return list(ids)


def get_model_domain_project_ids_from_es(config=DEFAULT_LOCAL_ES_CONFIG):
    client = create_es_client(config)
    type = "model"
    query = {
        "query": {"bool": {"filter": [{"term": {"type": type}}]}},
        "_source": ["id"],
    }
    ids = []
    for hit in scan(client, index=ES_INDEX_DOMAIN_PROJECT, query=query):
        ids.append(hit["_source"]["id"])
    return ids


def delete_indicator_from_es(indicator_data_id, config=DEFAULT_LOCAL_ES_CONFIG):
    # Remove existing indicator metadata in ES
    payload = {"query": {"term": {"data_id": indicator_data_id}}}
    auth = None
    if config.get("user"):
        auth = HTTPBasicAuth(config.get("user"), config.get("pwd"))
    try:
        res = requests.post(
            f"{config.get('url')}/data-datacube/_delete_by_query", auth=auth, json=payload
        )
        res.raise_for_status()
        result = res.json()
        # explicitly wait for index to be refreshed since delete_by_query doesn't support refresh=wait_for
        time.sleep(2)
        print(
            f">> ES: Metadata successfully deleted for indicator with data_id, {indicator_data_id}"
        )
    except Exception as exc:
        print(f">> ES: Error removing existing indicator metadata for {indicator_data_id}")
        raise


def get_indicator_dataset_ids_diff(source: ESConnectionConfig, dest: ESConnectionConfig):
    source_ids = set(get_data_ids_from_es(config=source))
    dest_ids = set(get_data_ids_from_es(config=dest))
    # dataset ids in the source but not in destination
    diffs = source_ids - dest_ids
    return list(diffs)


def get_model_domain_project_ids_diff(source: ESConnectionConfig, dest: ESConnectionConfig):
    source_ids = set(get_model_domain_project_ids_from_es(config=source))
    dest_ids = set(get_model_domain_project_ids_from_es(config=dest))
    # dataset ids in the source but not in destination
    diffs = source_ids - dest_ids
    return list(diffs)


def get_model_ids_diff(source: ESConnectionConfig, dest: ESConnectionConfig):
    source_ids = set(get_data_ids_from_es(type="model", config=source))
    dest_ids = set(get_data_ids_from_es(type="model", config=dest))
    # dataset ids in the source but not in destination
    diffs = source_ids - dest_ids
    return list(diffs)


def get_model_run_ids_diff(source: ESConnectionConfig, dest: ESConnectionConfig):
    source_ids = set(get_model_run_ids_from_es(config=source, prefix_model_id=False))
    dest_ids = set(get_model_run_ids_from_es(config=dest, prefix_model_id=False))

    # dataset ids in the source but not in destination
    diffs = source_ids - dest_ids
    return list(diffs)


def copy_documents(
    source: ESConnectionConfigWithIndex,
    destination: ESConnectionConfigWithIndex,
    doc_ids: list[str],
):
    """
    Copy documents from one Elasticsearch index to another.
    """
    source_client = create_es_client(source)
    destination_client = create_es_client(destination)
    for doc_id in doc_ids:
        try:
            # Fetch the document from the source index
            source_document = source_client.get(index=source["index"], id=doc_id)
            source_document_data = source_document["_source"]
            # Index (copy) the document to the destination index
            response = destination_client.index(
                index=destination["index"],
                id=source_document_data["id"],
                document=source_document_data,
            )

            if response.get("result") in ["created", "updated"]:
                print(
                    f"Document with id {doc_id}, copied successfully to {destination['index']} ({response.get('result')})."
                )
            else:
                print(f"Failed to copy the document, {doc_id}.")
        except Exception as e:
            print(f"Error: {e}")


def transform_reindex(
    source: ESConnectionConfigWithIndex,
    destination: ESConnectionConfigWithIndex,
    query=None,
    transform=None,
):
    """
    Reindex documents from source index to destination index.
    """
    source_client = create_es_client(source)
    destination_client = create_es_client(destination)
    docs = scan(source_client, index=source["index"], query=query)

    def _update_doc(hits):
        for h in hits:
            h["_index"] = destination["index"]
            if transform is not None:
                transformed = transform(
                    h["_source"], source_client=source_client, destination_client=destination_client
                )
                h["_source"] = transformed
                h["_id"] = transformed["id"]
            yield h

    # Note: Reduced chunk size from default 500 to 250, since the write requests failed with timeout sometimes.
    # `bulk` tries to process data by chunk and if a chunk size is too big and takes longer to process, it times out.
    # In that case reducing chunk size will help. Alternatively you can give the larger request_timeout value
    # e.g. bulk(destination_client, _update_doc(docs), request_timeout=60 * 3)
    (success, errors) = bulk(destination_client, _update_doc(docs), chunk_size=250)
    return (success, errors)


def copy_project(
    source: ESConnectionConfig,
    destination: ESConnectionConfig,
    project_id: str,
    new_project_name="",
):
    """
    Copy project and all the resources linked to the project from source ES to destination ES
    """
    new_project_id = "project-" + str(uuid.uuid4())
    id_query = {"query": {"term": {"id": project_id}}}
    project_id_query = {"query": {"term": {"project_id": project_id}}}

    def _duplicate_project(doc):
        doc["id"] = new_project_id
        if new_project_name:
            doc["name"] = new_project_name
        return doc

    def _assign_new_project_id(doc):
        # Assign new uuid to ID
        new_id = str(uuid.uuid4())
        doc["id"] = new_id
        doc["project_id"] = new_project_id
        return doc

    (success, _) = transform_reindex(
        {**source, "index": ES_INDEX_PROJECT},
        {**destination, "index": ES_INDEX_PROJECT},
        query=id_query,
        transform=_duplicate_project,
    )
    if success != 1:
        raise Exception(f"Failed to find or update the project with project ID, '{project_id}'")

    transform_reindex(
        {**source, "index": ES_INDEX_ANALYSIS},
        {**destination, "index": ES_INDEX_ANALYSIS},
        query=project_id_query,
        transform=_assign_new_project_id,
    )
    transform_reindex(
        {**source, "index": ES_INDEX_INSIGHT},
        {**destination, "index": ES_INDEX_INSIGHT},
        query=project_id_query,
        transform=_assign_new_project_id,
    )
    transform_reindex(
        {**source, "index": ES_INDEX_QUESTION},
        {**destination, "index": ES_INDEX_QUESTION},
        query=project_id_query,
        transform=_assign_new_project_id,
    )


def copy_analyses(
    config: ESConnectionConfig,
    source_project_id: str,
    dest_project_id: str,
):
    def _assign_dest_project_id(doc, source_client, destination_client):
        # Assign new uuid to ID
        new_id = str(uuid.uuid4())
        doc["id"] = new_id
        doc["project_id"] = dest_project_id
        return doc

    query = {
        "query": {
            "bool": {
                "filter": [
                    # Search for analyses in the source project
                    {"term": {"project_id": source_project_id}},
                ],
            }
        }
    }

    transform_reindex(
        {**config, "index": ES_INDEX_ANALYSIS},
        {**config, "index": ES_INDEX_ANALYSIS},
        query=query,
        transform=_assign_dest_project_id,
    )
