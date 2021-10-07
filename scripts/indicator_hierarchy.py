import time

from prefect.utilities.debug import raise_on_exception
from dask import delayed
from typing import Tuple
import dask.dataframe as dd
import dask.bytes as db
import pandas as pd
import requests
import boto3
import os
import json
import logging

from prefect import task, Flow, Parameter
from prefect.engine.signals import SKIP
from prefect.storage import Docker
from prefect.executors import DaskExecutor, LocalDaskExecutor


TRUE_TOKENS = ("true", "1", "t")

# address of the dask scheduler to connect to - set this to empty to spawn a local
# dask cluster
DASK_SCHEDULER = os.getenv("WM_DASK_SCHEDULER", "10.65.18.58:8786")

# run the flow locally without the prefect agent and server
LOCAL_RUN = os.getenv("WM_LOCAL", "False").lower() in TRUE_TOKENS

# write to the local file system - mostly to support testing
DEST_TYPE = os.getenv("WM_DEST_TYPE", "s3").lower()

# indicate whether or not this flow should be pushed to the docker registry as part of its
# registration process
PUSH_IMAGE = os.getenv("WM_PUSH_IMAGE", "False").lower() in TRUE_TOKENS

# Following env vars provide the defaults assigned to the prefect flow parameters.  The parameters
# are normally set when a run is scheduled in the deployment environment, but when we do a local
# run (WM_LOCAL set to true) the parameters will not be specified, and the default values are applied.
# Updating these env vars will therefore allow us to set local dev values.
#
# TODO: Safer to set these to reasonable dev values (or leave them empty) so that nothing is accidentally overwritten?

# elastic search output URL
ELASTIC_URL = os.getenv("WM_ELASTIC_URL", "http://10.65.18.34:9200")

# S3 data source URL
S3_SOURCE_URL = os.getenv("WM_S3_SOURCE_URL", "http://10.65.18.73:9000")

# S3 data destination URL
S3_DEST_URL = os.getenv("WM_S3_DEST_URL", "http://10.65.18.9:9000")

# default s3 indicator write bucket
S3_DEFAULT_INDICATOR_BUCKET = os.getenv("WM_S3_DEFAULT_INDICATOR_BUCKET", "indicators")

# default model s3 write bucket
S3_DEFAULT_MODEL_BUCKET = os.getenv("WM_S3_DEFAULT_MODEL_BUCKET", "models")

# This determines the number of bins(subtiles) per tile. Eg. Each tile has 4^6=4096 grid cells (subtiles) when LEVEL_DIFF is 6
# Tile (z, x, y) will have a sutbile where its zoom level is z + LEVEL_DIFF
# eg. Tile (9, 0, 0) will have (15, 0, 0) as a subtile with LEVEL_DIFF = 6
LEVEL_DIFF = 6

# Note: We need to figure out the spatial resolution of a run output in advance. For some model, 15 precision is way too high.
# For example, lpjml model covers the entire world in very coarse resolution and with 15 precision, it takes 1 hour to process and upload
# the tiles resulting 397395 tile files. (uploading takes most of the time )
# where it takes only a minitue with 10 precision. And having high precision tiles doesn't make
# significant difference visually since underlying data itself is very coarse.
MAX_SUBTILE_PRECISION = 14

MIN_SUBTILE_PRECISION = (
    LEVEL_DIFF  # since (0,0,0) main tile wil have (LEVEL_DIFF, x, y) subtiles as its grid cells
)

# Maximum zoom level for a main tile
MAX_ZOOM = MAX_SUBTILE_PRECISION - LEVEL_DIFF

ELASTIC_MODEL_RUN_INDEX = "data-model-run"
ELASTIC_INDICATOR_INDEX = "data-datacube"

LAT_LONG_COLUMNS = ["lat", "lng"]

REGION_LEVELS = ["country", "admin1", "admin2", "admin3"]


def extract_region_columns(df):
    columns = df.columns.to_list()
    # find the intersection
    result = list(set(REGION_LEVELS) & set(columns))
    # Re order the list by admin levels
    result.sort()
    if "country" in result:
        result.remove("country")
        result.insert(0, "country")
    return result


def join_region_columns(df, columns, level=3, deli="__"):
    regions = ["None", "None", "None", "None"]
    for region in columns:
        regions[REGION_LEVELS.index(region)] = df[region]

    if level == 3:
        return regions[0] + deli + regions[1] + deli + regions[2] + deli + regions[3]
    elif level == 2:
        return regions[0] + deli + regions[1] + deli + regions[2]
    elif level == 1:
        return regions[0] + deli + regions[1]
    else:
        return regions[0]


# save feature as a json file
def feature_to_json(hierarchy, dest, model_id, run_id, feature, writer):
    path = f"{model_id}/{run_id}/raw/{feature}/hierarchy/hierarchy.json"
    body = str(json.dumps(hierarchy))
    writer(body, path, dest)


# writes to S3 using the boto client
def write_to_s3(body, path, dest):
    # Create s3 client only if it hasn't been created in current worker
    # since initalizing the client is expensive. Make sure we only initialize it once per worker
    global s3
    if "s3" not in globals():
        s3 = boto3.session.Session().client(
            "s3",
            endpoint_url=dest["endpoint_url"],
            region_name=dest["region_name"],
            aws_access_key_id=dest["key"],
            aws_secret_access_key=dest["secret"],
        )

    try:
        s3.put_object(Body=body, Bucket=dest["bucket"], Key=path)
    except Exception as e:
        logging.error(f"failed to write bucket: {dest['bucket']} key: {path}")
        logging.error(e)


@task(log_stdout=True)
def download_data(source, data_paths):
    df = None
    # if source is from s3 bucket
    if "s3://" in data_paths[0]:
        df = dd.read_parquet(
            data_paths,
            storage_options={
                "anon": False,
                "use_ssl": False,
                "key": source["key"],
                "secret": source["secret"],
                "client_kwargs": {
                    "region_name": source["region_name"],
                    "endpoint_url": source["endpoint_url"],
                },
            },
        ).repartition(npartitions=12)
    else:
        # In some parquet files the value column will be type string. Filter out those parquet files and ignore for now
        numeric_files = []
        string_files = []
        for path in data_paths:
            if path.endswith("_str.parquet.gzip"):
                string_files.append(path)
            else:
                numeric_files.append(path)

        # Note: dask read_parquet doesn't work for gzip files. So here is the work around using pandas read_parquet
        dfs = [delayed(pd.read_parquet)(path) for path in numeric_files]
        df = dd.from_delayed(dfs).repartition(npartitions=12)

    # Remove lat/lng columns if they are null
    ll_df = df[LAT_LONG_COLUMNS]
    null_cols = set(ll_df.columns[ll_df.isnull().all()])
    if len(set(LAT_LONG_COLUMNS) & null_cols) > 0:
        print("No lat/long data. Dropping columns.")
        df = df.drop(columns=LAT_LONG_COLUMNS)

    # Drop all additional columns
    accepted_cols = set(
        ["timestamp", "country", "admin1", "admin2", "admin3", "lat", "lng", "feature", "value"]
    )
    all_cols = df.columns.to_list()
    cols_to_drop = list(set(all_cols) - accepted_cols)
    print(f"All columns: {all_cols}. Dropping: {cols_to_drop}")
    if len(cols_to_drop) > 0:
        df = df.drop(columns=cols_to_drop)

    # Ensure types
    df = df.astype({"value": "float64"})
    df.dtypes
    return df


@task(log_stdout=True)
def remove_null_region_columns(df):
    region_cols = extract_region_columns(df)
    cols_to_drop = set(df.columns[df.isnull().all()])
    region_cols_to_drop = list(cols_to_drop.intersection(region_cols))
    return df.drop(columns=region_cols_to_drop)


@task(log_stdout=True)
def record_region_hierarchy(df, dest, model_id, run_id):
    region_cols = extract_region_columns(df)
    if len(region_cols) == 0:
        raise SKIP("No regional information available")

    hierarchy = {}
    # This builds the hierarchy
    for _, row in df.iterrows():
        feature = row["feature"]
        if feature not in hierarchy:
            hierarchy[feature] = {}
        current_hierarchy_position = hierarchy[feature]

        # Not all rows will have values for all regions, find the last good region level
        last_region = None
        for region in reversed(region_cols):
            if row[region] is not None:
                last_region = region
                break
        if last_region is None:
            continue
        # Create list ending at the last good region. These are the levels we will have in our hierarchy
        last_level = REGION_LEVELS.index(last_region)

        # Add valid regions
        for level in range(last_level + 1):
            current_region = join_region_columns(row, region_cols, level)
            if (
                current_region not in current_hierarchy_position
                or current_hierarchy_position[current_region] is None
            ):
                current_hierarchy_position[current_region] = None if level == last_level else {}
            current_hierarchy_position = current_hierarchy_position[current_region]

    for feature in hierarchy:
        feature_to_json(hierarchy[feature], dest, model_id, run_id, feature, write_to_s3)


###########################################################################

with Flow("hierarchy-only") as flow:
    # Parameters
    model_id = Parameter("model_id", default="geo-test-data")
    run_id = Parameter("run_id", default="indicator")
    doc_ids = Parameter("doc_ids", default=[])
    data_paths = Parameter("data_paths", default=["s3://test/geo-test-data.parquet"])
    compute_tiles = Parameter("compute_tiles", default=False)
    output_qualifiers = Parameter("output_qualifiers", default={})
    indicator_bucket = Parameter("indicator_bucket", default=S3_DEFAULT_INDICATOR_BUCKET)
    model_bucket = Parameter("model_bucket", default=S3_DEFAULT_MODEL_BUCKET)

    source = Parameter(
        "source",
        default={
            "endpoint_url": S3_SOURCE_URL,
            "region_name": "us-east-1",
            "key": "foobar",
            "secret": "foobarbaz",
        },
    )

    dest = Parameter(
        "dest",
        default={
            "bucket": S3_DEFAULT_INDICATOR_BUCKET,
            "endpoint_url": S3_DEST_URL,
            "region_name": "us-east-1",
            "key": "foobar",
            "secret": "foobarbaz",
        },
    )

    raw_df = download_data(source, data_paths)

    df = remove_null_region_columns(raw_df)

    # ==== Compute high level features for current run =====
    record_region_hierarchy(df, dest, model_id, run_id)


total_start_time = time.time()
with raise_on_exception():
    jsons_dir = f"{os.getcwd()}/s3_jsons/"
    indicator_metadata_files = os.listdir(jsons_dir)
    indicator_metadata_files.sort()
    num_indicators = len(indicator_metadata_files)

    for index, file_name in enumerate(indicator_metadata_files):
        print(
            f">> Progess {index}/{num_indicators}. Started {int((time.time() - total_start_time) / 60)} minutes ago."
        )
        print(f">> Processing {file_name}")
        start_time = time.time()
        try:
            with open(f"{jsons_dir}{file_name}") as f:
                metadata = json.loads(f.read())
                state = flow.run(
                    executor=DaskExecutor(address=DASK_SCHEDULER),
                    parameters=dict(
                        model_id=metadata["id"],
                        run_id="indicator",
                        data_paths=metadata["data_paths"],
                    ),
                )
                if state.is_successful():
                    print(f'>> Successfully ingested {metadata["id"]}')
                else:
                    print(f'>> Ingest failed for {metadata["id"]}')
        except Exception as exc:
            print(f">> Error processing {file_name}")
            print(exc)
        print(">> Finished in %.2f seconds" % (time.time() - start_time))

print("## All completed in %.2f seconds" % (time.time() - total_start_time))
