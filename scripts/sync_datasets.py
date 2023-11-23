#!/usr/bin/env python3

import argparse
from dotenv import dotenv_values
from common import (
    copy_documents,
    get_model_domain_project_ids_diff,
    get_model_ids_diff,
    get_model_run_ids_diff,
    get_indicator_dataset_ids_diff,
    ES_INDEX_DOMAIN_PROJECT,
    ES_INDEX_DATACUBE,
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sync datasets, model runs and model metadata from the source environment to the destination environment",
        epilog="""Examples:
            """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "SOURCE_ENV_FILE",
        help="Source env file containing ES_URL, ES_USER, and ES_PWD env variables",
    )
    parser.add_argument(
        "DESTINATION_ENV_FILE",
        help="Destination env file containing ES_URL, ES_USER, and ES_PWD env variables",
    )

    args = parser.parse_args()

    # Load environment files to config object
    source_config = dotenv_values(dotenv_path=args.SOURCE_ENV_FILE)
    target_config = dotenv_values(dotenv_path=args.DESTINATION_ENV_FILE)

    source_es_config = {
        "url": source_config.get("ES_URL"),
        "user": source_config.get("ES_USER", ""),
        "pwd": source_config.get("ES_PWD", ""),
    }
    target_es_config = {
        "url": target_config.get("ES_URL"),
        "user": target_config.get("ES_USER", ""),
        "pwd": target_config.get("ES_PWD", ""),
    }

    model_domain_project_diffs = get_model_domain_project_ids_diff(
        source_es_config, target_es_config
    )
    model_diffs = get_model_ids_diff(
        source_es_config,
        target_es_config,
    )
    model_run_diffs = get_model_run_ids_diff(
        source_es_config,
        target_es_config,
    )

    indicator_diffs = get_indicator_dataset_ids_diff(
        source_es_config,
        target_es_config,
    )
    print("Following resources exist in the source environment but not in the target environment")
    print(f"\nModel domain projects: {len(model_domain_project_diffs)}")
    [print(v) for v in model_domain_project_diffs]

    print(f"\nModels (model datacubes): {len(model_diffs)}")
    [print(v) for v in model_diffs]

    print(f"\nModel runs: {len(model_run_diffs)}")
    [print(v) for v in model_run_diffs]

    print(f"\nIndicator datasets: {len(indicator_diffs)}")
    [print(v) for v in indicator_diffs]

    # Copy domain project and model metadata
    if len(model_domain_project_diffs) > 0 or len(model_diffs):
        print("Syncing model metadata...")
        copy_documents(
            {**source_es_config, "index": ES_INDEX_DOMAIN_PROJECT},
            {**target_es_config, "index": ES_INDEX_DOMAIN_PROJECT},
            model_domain_project_diffs,
        )
        copy_documents(
            {**source_es_config, "index": ES_INDEX_DATACUBE},
            {**target_es_config, "index": ES_INDEX_DATACUBE},
            model_diffs,
        )

    # Submit data pipeline runs for model runs and indicator datasets
    if len(model_run_diffs) > 0:
        print("\nSubmitting data pipeline runs for model runs...")
        for rid in model_run_diffs:
            print(rid)

    if len(indicator_diffs) > 0:
        print("\nSubmitting data pipeline runs for indicator datasets...")
        for did in indicator_diffs:
            print(did)
