#!/usr/bin/env python3

import argparse
from os import listdir
from os.path import join
from typing import cast
from dotenv import dotenv_values
from common import ESConnectionConfigWithIndex, transform_reindex
from importlib import import_module

TRANSFORMS_DIR = "reindex_transforms"
TRANSFORM_FN_NAME = "transform_fn"

if __name__ == "__main__":
    available_transforms = [
        f.split(".")[0] for f in listdir(TRANSFORMS_DIR) if join(TRANSFORMS_DIR, f).endswith(".py")
    ]

    parser = argparse.ArgumentParser(
        description="Reindex allows you to reindex documents from one Elasticsearch index to another. It provides flexibility by allowing the application of custom transformation functions during the reindexing process.",
        epilog="""Trnasform Functions:

Custom transform functions can be defined in separate Python files within the "reindex_transforms" directory. Each file should contain a function named transform_fn that takes a document as input and returns the transformed document. The available transform functions can be listed using the -t or --transform option.

Examples:
# Basic reindexing from source to destination index
./reindex.py ./env/local.env project new-project

# Reindexing into the same index with a transform function
./reindex.py ./env/local.env project -t transform_fn

# Reindexing from source to the destination index in different Elasticsearch cluster
./reindex.py ./env/staging.env project new-project -d ./env/local.env -t transform_fn
""",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "ENV_FILE",
        help="Environment file containing Elasticsearch connection information (ES_URL, ES_USER, and ES_PWD variables).",
    )
    parser.add_argument(
        "SOURCE_INDEX_NAME",
        help="Source Elasticsearch index name from which documents will be reindexed.",
    )
    parser.add_argument(
        "DESTINATION_INDEX_NAME",
        nargs="?",
        default=None,
        help="Destination Elasticsearch index name. Defaults to 'SOURCE_INDEX_NAME'. This argument can be omitted when reindexing documents into the same index (self-transforming).",
    )
    parser.add_argument(
        "-d",
        "--dest-env",
        dest="dest_env",
        default=None,
        help="Destination Elasticsearch environment file. Use this option if reindexing into a different Elasticsearch cluster.",
    )
    parser.add_argument(
        "-t",
        "--transform",
        dest="transform",
        default=None,
        help='Transform function to be applied during reindexing. Choose from the available transforms in the "reindex_transforms" directory.',
        choices=available_transforms,
    )

    args = parser.parse_args()

    source_env_file = args.ENV_FILE
    dest_env_file = source_env_file if args.dest_env is None else args.dest_env

    source_index = args.SOURCE_INDEX_NAME
    dest_index = (
        source_index if args.DESTINATION_INDEX_NAME is None else args.DESTINATION_INDEX_NAME
    )

    # Load environment files to config object
    source_config = dotenv_values(dotenv_path=source_env_file)
    target_config = dotenv_values(dotenv_path=dest_env_file)

    source_es_config: ESConnectionConfigWithIndex = {
        "url": cast(str, source_config.get("ES_URL")),
        "user": cast(str, source_config.get("ES_USER", "")),
        "pwd": cast(str, source_config.get("ES_PWD", "")),
        "index": source_index,
    }
    dest_es_config: ESConnectionConfigWithIndex = {
        "url": cast(str, target_config.get("ES_URL")),
        "user": cast(str, target_config.get("ES_USER", "")),
        "pwd": cast(str, target_config.get("ES_PWD", "")),
        "index": dest_index,
    }

    transform_fn = None
    if args.transform is not None:
        transform_module = import_module(f"{TRANSFORMS_DIR}.{args.transform}")
        transform_fn = getattr(transform_module, TRANSFORM_FN_NAME)

    transform_reindex(source_es_config, dest_es_config, transform=transform_fn)
