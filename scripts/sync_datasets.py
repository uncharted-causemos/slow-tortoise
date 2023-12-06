#!/usr/bin/env python3

import argparse
from typing import cast
from dotenv import dotenv_values
from common import (
    copy_documents,
    get_model_domain_project_ids_diff,
    get_model_ids_diff,
    get_model_run_ids_diff,
    get_indicator_dataset_ids_diff,
    get_model_run_from_es,
    process_model_run,
    create_es_client,
    get_indicator_metadata_from_dojo,
    process_indicator,
    ESConnectionConfig,
    DojoApiConfig,
    CausemosApiConfig,
    ES_INDEX_DOMAIN_PROJECT,
    ES_INDEX_DATACUBE,
    ES_INDEX_MODEL_RUN,
)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Sync datasets, model runs, and model metadata between environments.",
        epilog="""Examples:
            ./sync_datasets.py ./env/aws.env ./env/local.env -c
            ./sync_datasets.py ./env/aws.env ./env/local.env -m
            ./sync_datasets.py ./env/aws.env ./env/local.env -md -l 100
            """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "SOURCE_ENV_FILE",
        help="Source environment file (ES_URL and DOJO_URL variables).",
    )
    parser.add_argument(
        "DESTINATION_ENV_FILE",
        help="Destination environment file (ES_URL and CAUSEMOS_URL variables).",
    )
    parser.add_argument(
        "-c",
        "--check",
        action="store_true",
        help="Print missing resources in the destination environment without copying data.",
    )
    parser.add_argument(
        "-m",
        "--model-runs",
        action="store_true",
        help="Submit data pipeline jobs for model runs in the destination environment.",
    )
    parser.add_argument(
        "-d",
        "--datasets",
        action="store_true",
        help="Submit data pipeline jobs for indicator datasets in the destination environment.",
    )
    parser.add_argument(
        "-l",
        "--limit",
        dest="limit",
        default=5,
        help="Maximum number of datasets or model runs to be processed. Defaults to 5.",
    )

    args = parser.parse_args()

    # Load environment files to config object
    source_config = dotenv_values(dotenv_path=args.SOURCE_ENV_FILE)
    target_config = dotenv_values(dotenv_path=args.DESTINATION_ENV_FILE)

    IS_TARGET_LOCAL_ENV = cast(str, target_config.get("ENV", "")).lower() == "local"

    selected_datapipeline_tasks = []
    if IS_TARGET_LOCAL_ENV:
        # If the target is local environment, skip 'compute_tiling' task
        selected_datapipeline_tasks = [
            "compute_global_timeseries",
            "compute_regional_stats",
            "compute_regional_timeseries",
            "compute_regional_aggregation",
        ]

    dojo_config: DojoApiConfig = {
        "url": cast(str, source_config.get("DOJO_URL")),
        "user": cast(str, source_config.get("DOJO_USER", "")),
        "pwd": cast(str, source_config.get("DOJO_PWD", "")),
    }
    target_causemos_config: CausemosApiConfig = {
        "url": cast(str, target_config.get("CAUSEMOS_URL")),
        "user": cast(str, target_config.get("CAUSEMOS_USER", "")),
        "pwd": cast(str, target_config.get("CAUSEMOS_PWD", "")),
    }
    source_es_config: ESConnectionConfig = {
        "url": cast(str, source_config.get("ES_URL")),
        "user": cast(str, source_config.get("ES_USER", "")),
        "pwd": cast(str, source_config.get("ES_PWD", "")),
    }
    target_es_config: ESConnectionConfig = {
        "url": cast(str, target_config.get("ES_URL")),
        "user": cast(str, target_config.get("ES_USER", "")),
        "pwd": cast(str, target_config.get("ES_PWD", "")),
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

    # If check option is provided, exit without syncing data. Just prints out the differences
    if args.check:
        exit()

    # Copy over model domain projects and model metadata
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

    # Submit data pipeline runs for model runs
    if args.model_runs and len(model_run_diffs) > 0:
        print("\nSubmitting data pipeline runs for model runs...")
        target_es_client = create_es_client(target_es_config)
        for rid in model_run_diffs[: int(args.limit)]:
            print(f"Submitting model run {rid} for processing...")
            try:
                # Copy over model run metadata from source to target ES
                metadata = get_model_run_from_es(rid, config=source_es_config)

                # remove previous flow run metadata properties if already exists
                metadata.pop("flow_id", None)
                metadata.pop("runtimes", None)
                metadata["status"] = "SUBMITTED"

                res = target_es_client.index(
                    index=ES_INDEX_MODEL_RUN, id=metadata["id"], document=metadata, refresh=True
                )
                if res.get("result") in ["created", "updated"]:
                    process_model_run(
                        metadata,
                        selected_output_tasks=selected_datapipeline_tasks,
                        causemos_api_config=target_causemos_config,
                    )
                else:
                    raise Exception(f"Failed to index a document, {rid}")
            except Exception as e:
                print(f"Error: {e}")

    # Submit data pipeline runs for indicator datasets
    if args.datasets and len(indicator_diffs) > 0:
        print("\nSubmitting data pipeline runs for indicator datasets...")
        for did in indicator_diffs[: int(args.limit)]:
            print(f"Submitting dataset {did} for processing...")
            try:
                metadata = get_indicator_metadata_from_dojo(did, config=dojo_config)
                process_indicator(
                    metadata,
                    selected_output_tasks=selected_datapipeline_tasks,
                    causemos_api_config=target_causemos_config,
                )
            except Exception as e:
                print(f"Error: {e}")
