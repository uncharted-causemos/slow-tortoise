from dask import delayed
from typing import Tuple
import dask.dataframe as dd
import dask.bytes as db
import pandas as pd
import requests
import os

from prefect import task, Flow, Parameter
from prefect.engine.signals import SKIP, FAIL
from prefect.storage import Docker
from prefect.executors import DaskExecutor, LocalDaskExecutor
from flows.common import (
    run_temporal_aggregation,
    deg2num,
    ancestor_tiles,
    filter_by_min_zoom,
    tile_coord,
    save_tile,
    save_timeseries_as_csv,
    stats_to_json,
    info_to_json,
    to_proto,
    extract_region_columns,
    join_region_columns,
    save_regional_qualifiers_to_csv,
    write_regional_aggregation_csv,
    output_values_to_json_array,
    raw_data_to_json,
    compute_timeseries_by_region,
    write_to_file,
    write_to_null,
    write_to_s3,
    compute_subtile_stats,
    REGION_LEVELS,
    REQUIRED_COLS,
)

# The name of the flow when registered in Prefect
FLOW_NAME = "Data Pipeline"

# Maps a write type to writing function
WRITE_TYPES = {
    "s3": write_to_s3,  # write to an s3 bucket
    "file": write_to_file,  # write to the local file system
    "none": write_to_null,  # skip write for debugging
}

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

    print(df.dtypes)
    print(df.head())

    # Remove lat/lng columns if they are null
    ll_df = df[LAT_LONG_COLUMNS]
    null_cols = set(ll_df.columns[ll_df.isnull().all()])
    if len(set(LAT_LONG_COLUMNS) & null_cols) > 0:
        print("No lat/long data. Dropping columns.")
        df = df.drop(columns=LAT_LONG_COLUMNS)

    if len(df.index) == 0:
        raise FAIL("DataFrame has no rows")

    # Ensure types
    df = df.astype({"value": "float64"})
    return df


@task(log_stdout=True)
def configure_pipeline(
    df, dest, indicator_bucket, model_bucket, compute_tiles, is_indicator
) -> Tuple[dict, str, bool, bool, bool, bool, bool]:
    all_cols = df.columns.to_list()
    compute_tiles = compute_tiles and set(LAT_LONG_COLUMNS).issubset(all_cols)

    if is_indicator:
        dest["bucket"] = indicator_bucket
        elastic_index = ELASTIC_INDICATOR_INDEX
        compute_raw = True
        compute_monthly = True
        compute_annual = True
        compute_summary = False
    else:
        dest["bucket"] = model_bucket
        elastic_index = ELASTIC_MODEL_RUN_INDEX
        compute_raw = False
        compute_monthly = True
        compute_annual = True
        compute_summary = True

    return (
        dest,
        elastic_index,
        compute_raw,
        compute_monthly,
        compute_annual,
        compute_summary,
        compute_tiles,
    )


@task(log_stdout=True)
def save_raw_data(df, dest, time_res, model_id, run_id, should_run):
    if should_run is False:
        raise SKIP("Saving raw data was not requested")

    raw_df = df.copy()
    output_columns = ["timestamp", "country", "admin1", "admin2", "admin3", "value"]

    raw_df = raw_df.groupby(["feature"]).apply(
        lambda x: raw_data_to_json(
            x[output_columns],
            dest,
            model_id,
            run_id,
            time_res,
            x["feature"].values[0],
            WRITE_TYPES[DEST_TYPE],
        ),
        meta=(None, "object"),
    )
    raw_df.compute()


@task(log_stdout=True)
def process_null_columns(df):
    # Drop a column if all values are null
    exclude_columns = set(["timestamp", "lat", "lng", "feature", "value"])
    null_cols = set(df.columns[df.isnull().all()])
    cols_to_drop = list(null_cols - exclude_columns)
    df = df.drop(columns=cols_to_drop)

    # In the remaining columns, fill all null values with "None"
    # TODO: When adding support for different qualifier roles, we will need to fill numeric roles with something else
    remaining_columns = list(set(df.columns.to_list()) - exclude_columns - null_cols)
    df[remaining_columns] = df[remaining_columns].fillna(value="None", axis=1).astype("str")
    return df


@task(skip_on_upstream_skip=False)
def temporal_aggregation(df, time_res, should_run):
    if should_run is False:
        raise SKIP(f"Aggregating for resolution {time_res} was not requested")
    return run_temporal_aggregation(df, time_res)


@task(log_stdout=True)
def compute_timeseries_as_csv(
    df, dest, time_res, model_id, run_id, qualifier_map, qualifer_columns
):
    qualifier_cols = [*qualifer_columns, []]  # [] is the default case of ignoring qualifiers

    # persist the result in memory since this df is going to be used for multiple qualifiers
    df = df.persist()

    # Iterate through all the qualifier columns. Not all columns map to
    # all the features, they will be excluded when processing each feature
    for qualifier_col in qualifier_cols:
        timeseries_df = df.copy()
        timeseries_aggs = ["min", "max", "sum", "mean"]
        timeseries_lookup = {
            ("t_sum", "min"): "s_min_t_sum",
            ("t_sum", "max"): "s_max_t_sum",
            ("t_sum", "sum"): "s_sum_t_sum",
            ("t_sum", "mean"): "s_mean_t_sum",
            ("t_mean", "min"): "s_min_t_mean",
            ("t_mean", "max"): "s_max_t_mean",
            ("t_mean", "sum"): "s_sum_t_mean",
            ("t_mean", "mean"): "s_mean_t_mean",
        }
        timeseries_agg_columns = [
            "s_min_t_sum",
            "s_max_t_sum",
            "s_sum_t_sum",
            "s_mean_t_sum",
            "s_min_t_mean",
            "s_max_t_mean",
            "s_sum_t_mean",
            "s_mean_t_mean",
        ]

        timeseries_df = timeseries_df.groupby(["feature", "timestamp"] + qualifier_col).agg(
            {"t_sum": timeseries_aggs, "t_mean": timeseries_aggs}
        )
        timeseries_df.columns = timeseries_df.columns.to_flat_index()
        timeseries_df = timeseries_df.rename(columns=timeseries_lookup).reset_index()
        timeseries_df = timeseries_df.groupby(["feature"]).apply(
            lambda x: save_timeseries_as_csv(
                x,
                qualifier_col,
                dest,
                model_id,
                run_id,
                time_res,
                timeseries_agg_columns,
                qualifier_map,
                WRITE_TYPES[DEST_TYPE],
            ),
            meta=(None, "object"),
        )
        timeseries_df.compute()


@task(log_stdout=True)
def compute_regional_aggregation_to_csv(
    input_df, dest, time_res, model_id, run_id, qualifier_map, qualifer_columns
):
    qualifier_cols = [*qualifer_columns, []]  # [] is the default case of ignoring qualifiers

    # Copy input df so that original df doesn't get mutated
    df = input_df.copy()
    # Ranme columns
    df.columns = df.columns.str.replace("t_sum", "s_sum_t_sum").str.replace(
        "t_mean", "s_sum_t_mean"
    )
    df["s_count"] = 1
    df = df.reset_index()

    regions_cols = extract_region_columns(df)
    if len(regions_cols) == 0:
        raise SKIP("No regional information available")

    # persist the result in memory since this df is going to be used for multiple qualifiers
    df = df.persist()

    # Iterate through all the qualifier columns. Not all columns map to
    # all the features, they will be excluded when processing each feature
    for qualifier_col in qualifier_cols:
        qualifier_df = df.copy()

        # Region aggregation at the highest admin level
        qualifier_df = (
            qualifier_df[
                ["feature", "timestamp", "s_sum_t_sum", "s_sum_t_mean", "s_count"]
                + regions_cols
                + qualifier_col
            ]
            .groupby(["feature", "timestamp"] + regions_cols + qualifier_col)
            .agg(["sum"])
        )
        qualifier_df.columns = qualifier_df.columns.droplevel(1)
        qualifier_df = qualifier_df.reset_index()

        # persist the result in memory at this point since this df is going to be used multiple times to compute for different regional levels
        qualifier_df = qualifier_df.persist()

        # Compute aggregation and save for all regional levels
        for level in range(len(regions_cols)):
            save_df = qualifier_df.copy()
            # Merge region columns to single region_id column. eg. ['Ethiopia', 'Afar'] -> ['Ethiopia_Afar']
            save_df["region_id"] = join_region_columns(save_df, regions_cols, level)

            if len(qualifier_col) == 0:
                # Compute regional stats
                assist_compute_stats(
                    save_df.copy(),
                    dest,
                    time_res,
                    model_id,
                    run_id,
                    f"regional/{regions_cols[level]}",
                )

            desired_columns = [
                "feature",
                "timestamp",
                "region_id",
                "s_sum_t_sum",
                "s_sum_t_mean",
                "s_count",
            ] + qualifier_col
            save_df = save_df[desired_columns].groupby(["feature", "timestamp"]).agg(list)

            save_df = save_df.reset_index()
            # At this point data is already reduced to reasonably small size due to prior admin aggregation.
            # Just perform repartition to make sure save io operation runs in parallel since each writing operation is expensive and blocks
            # Set npartitions to same as # of available workers/threads. Increasing partition number beyond the number of the workers doesn't seem to give more performance benefits.
            save_df = save_df.repartition(npartitions=12)

            if len(qualifier_col) == 0:
                foo = save_df.apply(
                    lambda x: write_regional_aggregation_csv(
                        x,
                        dest,
                        model_id,
                        run_id,
                        time_res,
                        WRITE_TYPES[DEST_TYPE],
                        region_level=regions_cols[level],
                    ),
                    axis=1,
                    meta=(None, "object"),
                )
                foo.compute()
            else:
                foo = save_df.apply(
                    lambda x: save_regional_qualifiers_to_csv(
                        x,
                        qualifier_col[0],
                        dest,
                        model_id,
                        run_id,
                        time_res,
                        qualifier_map,
                        WRITE_TYPES[DEST_TYPE],
                        region_level=regions_cols[level],
                    ),
                    axis=1,
                    meta=(None, "object"),
                )
                foo.compute()


@task(log_stdout=True)
def get_qualifier_columns(df):
    base_cols = REQUIRED_COLS
    all_cols = df.columns.to_list()
    qualifier_cols = [[col] for col in set(all_cols) - base_cols]
    return qualifier_cols


@task(log_stdout=True)
def compute_regional_timeseries(df, dest, time_res, model_id, run_id):
    regions_cols = extract_region_columns(df)
    for region_level in regions_cols:
        compute_timeseries_by_region(
            df, dest, model_id, run_id, time_res, region_level, WRITE_TYPES[DEST_TYPE]
        )


@task(log_stdout=True)
def subtile_aggregation(df, should_run):
    if should_run is False:
        raise SKIP("Tiling was not requested")

    # Spatial aggregation to the higest supported precision(subtile z) level

    stile = df.apply(
        lambda x: deg2num(x.lat, x.lng, MAX_SUBTILE_PRECISION), axis=1, meta=(None, "object")
    )
    subtile_df = df.assign(subtile=stile)
    subtile_df = (
        subtile_df[["feature", "timestamp", "subtile", "t_sum", "t_mean"]]
        .groupby(["feature", "timestamp", "subtile"])
        .agg(["sum", "count"])
    )

    # Rename columns
    spatial_lookup = {
        ("t_sum", "sum"): "s_sum_t_sum",
        ("t_sum", "count"): "s_count_t_sum",
        ("t_mean", "sum"): "s_sum_t_mean",
        ("t_mean", "count"): "s_count",
    }
    subtile_df.columns = subtile_df.columns.to_flat_index()
    subtile_df = (
        subtile_df.rename(columns=spatial_lookup).drop(columns="s_count_t_sum").reset_index()
    )
    return subtile_df


@task(log_stdout=True)
def compute_tiling(df, dest, time_res, model_id, run_id):
    stile = df.apply(
        lambda x: filter_by_min_zoom(ancestor_tiles(x.subtile), MIN_SUBTILE_PRECISION),
        axis=1,
        meta=(None, "object"),
    )
    tiling_df = df.assign(subtile=stile)
    tiling_df = tiling_df.explode("subtile").repartition(npartitions=100)

    # Assign main tile coord for each subtile
    tiling_df["tile"] = tiling_df.apply(
        lambda x: tile_coord(x.subtile, LEVEL_DIFF), axis=1, meta=(None, "object")
    )

    tiling_df = (
        tiling_df.groupby(["feature", "timestamp", "tile"])
        .agg(list)
        .reset_index()
        .repartition(npartitions=200)
        .apply(
            lambda x: save_tile(
                to_proto(x),
                dest,
                model_id,
                run_id,
                x.feature,
                time_res,
                x.timestamp,
                WRITE_TYPES[DEST_TYPE],
            ),
            axis=1,
            meta=(None, "object"),
        )
    )  # convert each row to protobuf and save
    tiling_df.compute()


@task(log_stdout=True)
def compute_stats(df, dest, time_res, model_id, run_id, filename):
    compute_subtile_stats(
        df, dest, model_id, run_id, time_res, MIN_SUBTILE_PRECISION, WRITE_TYPES[DEST_TYPE]
    )


def assist_compute_stats(df, dest, time_res, model_id, run_id, filename):
    # Compute mean and get new dataframe with mean columns added
    stats_df = df.assign(
        s_mean_t_sum=df["s_sum_t_sum"] / df["s_count"],
        s_mean_t_mean=df["s_sum_t_mean"] / df["s_count"],
    )
    # Stats aggregation
    stats_aggs = ["min", "max"]
    stats_lookup = {
        ("s_sum_t_sum", "min"): "min_s_sum_t_sum",
        ("s_sum_t_sum", "max"): "max_s_sum_t_sum",
        ("s_mean_t_sum", "min"): "min_s_mean_t_sum",
        ("s_mean_t_sum", "max"): "max_s_mean_t_sum",
        ("s_sum_t_mean", "min"): "min_s_sum_t_mean",
        ("s_sum_t_mean", "max"): "max_s_sum_t_mean",
        ("s_mean_t_mean", "min"): "min_s_mean_t_mean",
        ("s_mean_t_mean", "max"): "max_s_mean_t_mean",
    }
    stats_agg_columns = [
        "min_s_sum_t_sum",
        "max_s_sum_t_sum",
        "min_s_mean_t_sum",
        "max_s_mean_t_sum",
        "min_s_sum_t_mean",
        "max_s_sum_t_mean",
        "min_s_mean_t_mean",
        "max_s_mean_t_mean",
    ]

    stats_df = stats_df.groupby(["feature"]).agg(
        {
            "s_sum_t_sum": stats_aggs,
            "s_mean_t_sum": stats_aggs,
            "s_sum_t_mean": stats_aggs,
            "s_mean_t_mean": stats_aggs,
        }
    )
    stats_df.columns = stats_df.columns.to_flat_index()
    stats_df = stats_df.rename(columns=stats_lookup).reset_index()
    stats_df = stats_df.groupby(["feature"]).apply(
        lambda x: stats_to_json(
            x[stats_agg_columns],
            dest,
            model_id,
            run_id,
            x["feature"].values[0],
            time_res,
            filename,
            WRITE_TYPES[DEST_TYPE],
        ),
        meta=(None, "object"),
    )
    stats_df.compute()


@task(log_stdout=True)
def compute_output_summary(df):
    # Timeseries aggregation
    timeseries_aggs = ["mean"]
    timeseries_agg_column = "s_mean_t_mean"
    timeseries_lookup = {("t_mean", "mean"): timeseries_agg_column}

    timeseries_df = df.groupby(["feature", "timestamp"]).agg({"t_mean": timeseries_aggs})
    timeseries_df.columns = timeseries_df.columns.to_flat_index()
    timeseries_df = timeseries_df.rename(columns=timeseries_lookup).reset_index()

    summary = output_values_to_json_array(
        timeseries_df[["feature", timeseries_agg_column]], timeseries_agg_column
    )
    return summary


@task(skip_on_upstream_skip=False, log_stdout=True)
def update_metadata(doc_ids, summary_values, elastic_url, elastic_index):
    data = {"doc": {"status": "READY"}}
    if summary_values is not None:
        data["doc"]["output_agg_values"] = summary_values

    for doc_id in doc_ids:
        r = requests.post(f"{elastic_url}/{elastic_index}/_update/{doc_id}", json=data)
        print(r.text)
        r.raise_for_status()


@task(log_stdout=True)
def record_region_lists(df, dest, model_id, run_id):
    def cols_to_lists(df, id_cols, feature):
        lists = {region: [] for region in REGION_LEVELS}
        for index, id_col in enumerate(id_cols):
            lists[REGION_LEVELS[index]] = df[id_col].unique().tolist()
        info_to_json(
            lists,
            dest,
            model_id,
            run_id,
            feature,
            "region_lists",
            WRITE_TYPES[DEST_TYPE],
        )

    region_cols = extract_region_columns(df)
    if len(region_cols) == 0:
        raise SKIP("No regional information available")

    save_df = df.copy()

    # ["__region_id_0", "__region_id_1", "__region_id_2", "__region_id_3"]
    id_cols = [f"__region_id_{level}" for level in range(len(region_cols))]
    for index, id_col in enumerate(id_cols):
        save_df[id_col] = join_region_columns(save_df, region_cols, index)

    foo = (
        save_df[["feature"] + id_cols]
        .groupby(["feature"])
        .apply(
            lambda x: cols_to_lists(x, id_cols, x["feature"].values[0]),
            meta=(None, "object"),
        )
    )
    foo.compute()


@task(log_stdout=True)
def record_qualifier_lists(df, dest, model_id, run_id, qualifiers):
    save_df = df.copy()
    qualifier_columns = sum(qualifiers, [])

    foo = (
        save_df[["feature"] + qualifier_columns]
        .groupby(["feature"])
        .apply(
            lambda x: info_to_json(
                {col: x[col].unique().tolist() for col in qualifier_columns},
                dest,
                model_id,
                run_id,
                x["feature"].values[0],
                "qualifier_lists",
                WRITE_TYPES[DEST_TYPE],
            ),
            meta=(None, "object"),
        )
    )
    foo.compute()


###########################################################################

with Flow(FLOW_NAME) as flow:
    # setup the flow executor - if no adress is set rely on a local dask instance
    if not DASK_SCHEDULER:
        flow.executor = LocalDaskExecutor()
    else:
        flow.executor = DaskExecutor(DASK_SCHEDULER)

    # setup the flow storage - will build a docker image containing the flow from the base image
    # provided
    registry_url = "docker.uncharted.software"
    image_name = "worldmodeler/wm-data-pipeline/datacube-ingest-docker-test"
    if not PUSH_IMAGE:
        image_name = f"{registry_url}/{image_name}"
        registry_url = ""

    flow.storage = Docker(
        registry_url=registry_url,
        base_image="docker.uncharted.software/worldmodeler/wm-data-pipeline:latest",
        image_name=image_name,
        local_image=True,
        stored_as_script=True,
        path="/wm_data_pipeline/flows/data_pipeline.py",
        ignore_healthchecks=True,
    )

    # Parameters
    model_id = Parameter("model_id", default="geo-test-data")
    run_id = Parameter("run_id", default="test-run")
    doc_ids = Parameter("doc_ids", default=[])
    data_paths = Parameter("data_paths", default=["s3://test/geo-test-data.parquet"])
    compute_tiles = Parameter("compute_tiles", default=False)
    is_indicator = Parameter("is_indicator", default=False)
    qualifier_map = Parameter("qualifier_map", default={})
    indicator_bucket = Parameter("indicator_bucket", default=S3_DEFAULT_INDICATOR_BUCKET)
    model_bucket = Parameter("model_bucket", default=S3_DEFAULT_MODEL_BUCKET)

    # skip write to elastic if URL unset - we define this and don't use it prefect
    # errors
    if ELASTIC_URL:
        elastic_url = Parameter("elastic_url", default=ELASTIC_URL)

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
            "endpoint_url": S3_DEST_URL,
            "region_name": "us-east-1",
            "key": "foobar",
            "secret": "foobarbaz",
        },
    )

    raw_df = download_data(source, data_paths)

    # ==== Set parameters that determine which tasks should run based on the type of data we're ingesting ====
    (
        dest,
        elastic_index,
        compute_raw,
        compute_monthly,
        compute_annual,
        compute_summary,
        compute_tiles,
    ) = configure_pipeline(
        raw_df, dest, indicator_bucket, model_bucket, compute_tiles, is_indicator
    )

    # ==== Save raw data =====
    save_raw_data(raw_df, dest, "raw", model_id, "indicator", compute_raw)

    df = process_null_columns(raw_df)

    qualifier_columns = get_qualifier_columns(df)

    # ==== Compute lists of all gadm regions and qualifier values =====
    record_region_lists(df, dest, model_id, run_id)
    record_qualifier_lists(df, dest, model_id, run_id, qualifier_columns)

    # ==== Run aggregations based on monthly time resolution =====
    monthly_data = temporal_aggregation(df, "month", compute_monthly)
    month_csv_ts_done = compute_timeseries_as_csv(
        monthly_data, dest, "month", model_id, run_id, qualifier_map, qualifier_columns
    )
    compute_regional_timeseries(monthly_data, dest, "month", model_id, run_id)
    monthly_csv_regional_df = compute_regional_aggregation_to_csv(
        monthly_data, dest, "month", model_id, run_id, qualifier_map, qualifier_columns
    )

    monthly_spatial_data = subtile_aggregation(
        monthly_data, compute_tiles, upstream_tasks=[month_csv_ts_done]
    )
    month_stats_done = compute_stats(monthly_spatial_data, dest, "month", model_id, run_id, "stats")
    month_done = compute_tiling(
        monthly_spatial_data, dest, "month", model_id, run_id, upstream_tasks=[month_stats_done]
    )

    # ==== Run aggregations based on annual time resolution =====
    annual_data = temporal_aggregation(df, "year", compute_annual, upstream_tasks=[monthly_csv_regional_df, month_done])
    year_csv_ts_done = compute_timeseries_as_csv(
        annual_data, dest, "year", model_id, run_id, qualifier_map, qualifier_columns
    )
    compute_regional_timeseries(annual_data, dest, "year", model_id, run_id)
    annual_csv_regional_df = compute_regional_aggregation_to_csv(
        annual_data, dest, "year", model_id, run_id, qualifier_map, qualifier_columns
    )

    annual_spatial_data = subtile_aggregation(
        annual_data, compute_tiles, upstream_tasks=[year_csv_ts_done]
    )
    year_stats_done = compute_stats(annual_spatial_data, dest, "year", model_id, run_id, "stats")
    year_done = compute_tiling(
        annual_spatial_data, dest, "year", model_id, run_id, upstream_tasks=[year_stats_done]
    )

    # ==== Generate a single aggregate value per feature =====
    summary_data = temporal_aggregation(
        df,
        "all",
        compute_summary,
        upstream_tasks=[year_done, year_csv_ts_done, annual_csv_regional_df],
    )
    summary_values = compute_output_summary(summary_data)

    # ==== Update document in ES setting the status to READY =====
    if ELASTIC_URL:
        # TODO: Additional info to output
        # compute_tiles -> is there tile data computed
        # df["feature"].unique().tolist() -> list of features
        update_metadata(doc_ids, summary_values, elastic_url, elastic_index)

    # TODO: Saving intermediate result as a file (for each feature) and storing in our minio might be useful.
    # Then same data can be used for producing tiles and also used for doing regional aggregation and other computation in other tasks.
    # In that way we can have one jupyter notbook or python module for each tasks

# If this is a local run, just execute the flow in process.  Setting WM_DASK_SCHEDULER="" will result in a local cluster
# being run as well.
if __name__ == "__main__" and LOCAL_RUN:
    from prefect.utilities.debug import raise_on_exception

    with raise_on_exception():
        # flow.run(parameters=dict(is_indicator=True, model_id='ACLED', run_id='indicator', data_paths=['s3://test/acled/acled-test.bin']))
        # flow.run(parameters=dict(compute_tiles=True, model_id='geo-test-data', run_id='test-run', data_paths=['s3://test/geo-test-data.parquet']))
        # flow.run(parameters=dict(
        #      is_indicator=True,
        #      model_id='1fb59bc8-a321-4981-8ec9-1041798ddb7e',
        #      run_id='indicator',
        #      data_paths=["https://jataware-world-modelers.s3.amazonaws.com/dev/indicators/1fb59bc8-a321-4981-8ec9-1041798ddb7e/1fb59bc8-a321-4981-8ec9-1041798ddb7e.parquet.gzip"]
        # ))
        # flow.run(parameters=dict( # Conflict model
        #      compute_tiles=True,
        #      model_id="9e896392-2639-4df6-b4b4-e1b1d4cf46ae",
        #      run_id="2dc64e9d-be17-471e-a24b-aeb9c1178313",
        #      data_paths=["https://jataware-world-modelers.s3.amazonaws.com/dmc_results_dev/2dc64e9d-be17-471e-a24b-aeb9c1178313/2dc64e9d-be17-471e-a24b-aeb9c1178313_9e896392-2639-4df6-b4b4-e1b1d4cf46ae.parquet.gzip"]
        # ))
        # flow.run(parameters=dict( # Malnutrition model
        #      compute_tiles=True,
        #      model_id='425f58a4-bbba-44d3-83f3-aba353fc7c64',
        #      run_id='db68a592-e456-465f-9785-86440f49e838',
        #      data_paths=['https://jataware-world-modelers.s3.amazonaws.com/dmc_results_dev/db68a592-e456-465f-9785-86440f49e838/db68a592-e456-465f-9785-86440f49e838_425f58a4-bbba-44d3-83f3-aba353fc7c64.parquet.gzip']
        # ))
        flow.run(
            parameters=dict(  # Maxhop
                compute_tiles=True,
                model_id="maxhop-v0.2",
                run_id="4675d89d-904c-466f-a588-354c047ecf72",
                data_paths=[
                    "https://jataware-world-modelers.s3.amazonaws.com/dmc_results/4675d89d-904c-466f-a588-354c047ecf72/4675d89d-904c-466f-a588-354c047ecf72_maxhop-v0.2.parquet.gzip"
                ],
            )
        )
        # flow.run(
        #     parameters=dict(
        #         is_indicator=True,
        #         qualifier_map={"fatalities": ["event_type", "sub_event_type", "source_scale"]},
        #         model_id="_qualifier-test",
        #         run_id="indicator",
        #         data_paths=[
        #             "/Users/mkozlowski/_Git/12ecb553-9c50-4f3e-b175-4e3819a2f37b.parquet.gzip"
        #         ],
        #     )
        # )
        # LPJmL
        # flow.run(
        #     parameters=dict(
        #         is_indicator=True,
        #         qualifier_map={},
        #         model_id="_hierarchy-test",
        #         run_id="indicator",
        #         data_paths=[
        #             "/Users/mkozlowski/_Git/f9ee6dc2-ef3b-46d6-90cb-ba317b926629_9f407b82-d2d2-4c38-a2c3-ae1bb483f476.1.parquet.gzip"
        #         ],
        #     )
        # )
