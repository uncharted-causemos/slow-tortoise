#!/usr/bin/env python3

import argparse
from typing import cast
from dotenv import dotenv_values
from common import ESConnectionConfig, copy_analyses

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Copies all analyses from one project to another within the same Elasticsearch instance.",
        epilog="""Examples:
# Basic project copying within the same Elasticsearch cluster
./copy_analyses.py ./env/local.env source_project_id dest_project_id
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "ENV_FILE",
        help="Environment file containing the Elasticsearch connection information for the ES cluster (ES_URL, ES_USER, and ES_PWD variables).",
    )
    parser.add_argument(
        "SOURCE_PROJECT_ID",
        help="Project ID of the source project in the 'project' Elasticsearch index from which documents will be copied.",
    )
    parser.add_argument(
        "DEST_PROJECT_ID",
        help="Project ID of the source project in the 'project' Elasticsearch index from which documents will be copied.",
    )

    args = parser.parse_args()

    env_file = args.ENV_FILE
    source_project_id = args.SOURCE_PROJECT_ID
    dest_project_id = args.DEST_PROJECT_ID

    # Load environment files to config object
    config = dotenv_values(dotenv_path=env_file)

    es_config: ESConnectionConfig = {
        "url": cast(str, config.get("ES_URL")),
        "user": cast(str, config.get("ES_USER", "")),
        "pwd": cast(str, config.get("ES_PWD", "")),
    }

    copy_analyses(es_config, source_project_id, dest_project_id)
