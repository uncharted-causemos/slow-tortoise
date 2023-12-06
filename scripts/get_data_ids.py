#!/usr/bin/env python3

import argparse
import common
from dotenv import dotenv_values

parser = argparse.ArgumentParser(
    description="Get ids of the indicator dataset or model runs queried from Causemos ES."
)
parser.add_argument(
    "-s",
    dest="status",
    default="READY",
    help="the status of the indicators or model runs to be queried. Available values are 'READY', 'PROCESSING', and 'PROCESSING FAILED'. (Defaults to 'READY')",
)
parser.add_argument(
    "-m",
    action="store_true",
    help="if provided, query model run ids instead of indicator dataset ids",
)
parser.add_argument(
    "-e",
    dest="env_file_path",
    default="./env/local.env",
    help="environment file path. (Defaults to './env/local.env')",
)
args = parser.parse_args()

# Load environment files to config object
config = dotenv_values(dotenv_path=args.env_file_path)

es_config = {
    "url": config.get("ES_URL"),
    "user": config.get("ES_USER", ""),
    "pwd": config.get("ES_PWD", ""),
}

data_ids = (
    common.get_model_run_ids_from_es(args.status, config=es_config)
    if args.m
    else common.get_data_ids_from_es(args.status, config=es_config)
)

# Print out the ids
print("\n".join(data_ids))
