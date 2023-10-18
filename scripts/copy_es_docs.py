#!/usr/bin/env python3

import argparse, textwrap
from dotenv import dotenv_values
from elasticsearch import Elasticsearch

def copy_documents(source, destination, doc_ids):
  source_client = Elasticsearch([source["url"]], http_auth=(source["user"], source["pwd"]))
  destination_client = Elasticsearch([destination["url"]], http_auth=(destination["user"], destination["pwd"]))
  for doc_id in doc_ids:
    try:
        # Fetch the document from the source index
        source_document = source_client.get(index=source["index"], id=doc_id)
        source_document_data = source_document['_source']
        # Index (copy) the document to the destination index
        response = destination_client.index(index=destination["index"], id=source_document_data["id"], document=source_document_data)

        if response.get('result') in ['created', 'updated']:
            print(f"Document with id {doc_id}, copied successfully to {destination['index']} ({response.get('result')}).")
        else:
            print(f"Failed to copy the document, {doc_id}.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Copy Elasticsearch documents from the source index to the destination index overwriting existing documents.",
                                    epilog="""Examples:
                        ./copy_es_docs.py ./env/modeler.env domain-project ./env/analyst.env domain-project 7667b75f-6466-4577-a7c7-b95c9538a140 570a7779-3f23-4100-939c-4050a98586cf
                        ./copy_es_docs.py ./env/modeler.env data-datacube ./env/analyst.env data-datacube 5cf84e06-c1ce-4a9d-a2e6-ede687293a26
            """,
            formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("SOURCE_ENV_FILE", help="Source env file containing ES_URL, ES_USER, and ES_PWD env variables")
    parser.add_argument("SOURCE_INDEX", help="Source Elasticsearch index")
    parser.add_argument("DESTINATION_ENV_FILE", help="Destination env file containing ES_URL, ES_USER, and ES_PWD env variables")
    parser.add_argument("DESTINATION_INDEX", help="Destination Elasticsearch index")
    parser.add_argument("DOCUMENT_ID", nargs='+', help="Document ID to copy")

    args = parser.parse_args()

    # Load environment files to config object
    source_config = dotenv_values(dotenv_path=args.SOURCE_ENV_FILE)
    target_config = dotenv_values(dotenv_path=args.DESTINATION_ENV_FILE)

    source_es_config = {
        "url": source_config.get("ES_URL"),
        "user": source_config.get("ES_USER", ""),
        "pwd": source_config.get("ES_PWD", ""),
        "index": args.SOURCE_INDEX
    }
    target_es_config = {
        "url": target_config.get("ES_URL"),
        "user": target_config.get("ES_USER", ""),
        "pwd": target_config.get("ES_PWD", ""),
        "index": args.DESTINATION_INDEX
    }

    copy_documents(
        source_es_config,
        target_es_config,
        args.DOCUMENT_ID,
    )