#!/usr/bin/env python3

import argparse
from dotenv import dotenv_values
from common import copy_documents, ESConnectionConfigWithIndex

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Copy Elasticsearch documents from the source index to the destination index overwriting existing documents.",
        epilog="""Examples:
                        ./copy_es_docs.py ./env/modeler.env domain-project ./env/analyst.env domain-project 7667b75f-6466-4577-a7c7-b95c9538a140 570a7779-3f23-4100-939c-4050a98586cf
                        ./copy_es_docs.py ./env/modeler.env data-datacube ./env/analyst.env data-datacube 5cf84e06-c1ce-4a9d-a2e6-ede687293a26
            """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "SOURCE_ENV_FILE",
        help="Source env file containing ES_URL, ES_USER, and ES_PWD env variables",
    )
    parser.add_argument("SOURCE_INDEX", help="Source Elasticsearch index")
    parser.add_argument(
        "DESTINATION_ENV_FILE",
        help="Destination env file containing ES_URL, ES_USER, and ES_PWD env variables",
    )
    parser.add_argument("DESTINATION_INDEX", help="Destination Elasticsearch index")
    parser.add_argument("DOCUMENT_ID", nargs="+", help="Document ID to copy")

    args = parser.parse_args()

    # Load environment files to config object
    source_config = dotenv_values(dotenv_path=args.SOURCE_ENV_FILE)
    target_config = dotenv_values(dotenv_path=args.DESTINATION_ENV_FILE)

    source_es_config: ESConnectionConfigWithIndex = {
        "url": source_config.get("ES_URL"),
        "user": source_config.get("ES_USER", ""),
        "pwd": source_config.get("ES_PWD", ""),
        "index": args.SOURCE_INDEX,
    }
    target_es_config: ESConnectionConfigWithIndex = {
        "url": target_config.get("ES_URL"),
        "user": target_config.get("ES_USER", ""),
        "pwd": target_config.get("ES_PWD", ""),
        "index": args.DESTINATION_INDEX,
    }

    copy_documents(
        source_es_config,
        target_es_config,
        args.DOCUMENT_ID,
    )
