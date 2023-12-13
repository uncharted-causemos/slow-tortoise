#!/usr/bin/env python3

import argparse
from typing import cast
from dotenv import dotenv_values
from common import ESConnectionConfig, copy_project

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Copies a project and all linked resources from one Elasticsearch instance to another. It can also duplicate a project within a same Elasticsearch instance.",
        epilog="""Examples:
# Basic project copying within the same Elasticsearch cluster
./copy_project.py ./env/local.env source_project_id

# Project copying with a custom project name
./copy_project.py ./env/local.env source_project_id -n duplicated_project_name

# Project copying into a different Elasticsearch cluster
./copy_project.py ./env/staging.env source_project_id ./env/local.env
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "SOURCE_ENV_FILE",
        help="Environment file containing the Elasticsearch connection information for the source cluster (ES_URL, ES_USER, and ES_PWD variables).",
    )
    parser.add_argument(
        "SOURCE_PROJECT_ID",
        help="Project ID of the source project in the 'project' Elasticsearch index from which documents will be copied.",
    )
    parser.add_argument(
        "DESTINATION_ENV_FILE",
        default=None,
        nargs="?",
        help="Destination Elasticsearch environment file. Provide this if copying a project into a different Elasticsearch cluster.",
    )
    parser.add_argument(
        "-n",
        "--project-name",
        dest="project_name",
        default="",
        help="Name of the duplicated project. If not provided, the name will be the same as the source project name.",
    )

    args = parser.parse_args()

    source_env_file = args.SOURCE_ENV_FILE
    dest_env_file = (
        source_env_file if args.DESTINATION_ENV_FILE is None else args.DESTINATION_ENV_FILE
    )
    source_project_id = args.SOURCE_PROJECT_ID
    new_project_name: str = args.project_name

    # Load environment files to config object
    source_config = dotenv_values(dotenv_path=source_env_file)
    target_config = dotenv_values(dotenv_path=dest_env_file)

    source_es_config: ESConnectionConfig = {
        "url": cast(str, source_config.get("ES_URL")),
        "user": cast(str, source_config.get("ES_USER", "")),
        "pwd": cast(str, source_config.get("ES_PWD", "")),
    }
    dest_es_config: ESConnectionConfig = {
        "url": cast(str, target_config.get("ES_URL")),
        "user": cast(str, target_config.get("ES_USER", "")),
        "pwd": cast(str, target_config.get("ES_PWD", "")),
    }

    copy_project(source_es_config, dest_es_config, source_project_id, new_project_name)
