from dask import delayed
from typing import Tuple
import dask.dataframe as dd
import pandas as pd
import numpy as np
import os
import json
import re

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
    results_to_json,
    to_proto,
    extract_region_columns,
    join_region_columns,
    save_regional_qualifiers_to_csv,
    write_regional_aggregation_csv,
    output_values_to_json_array,
    raw_data_to_csv,
    compute_timeseries_by_region,
    write_to_file,
    write_to_null,
    write_to_s3,
    compute_subtile_stats,
    apply_qualifier_count_limit,
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

# S3 data source URL
S3_SOURCE_URL = os.getenv("WM_S3_SOURCE_URL", "http://10.65.18.73:9000")

# S3 data destination URL
S3_DEST_URL = os.getenv("WM_S3_DEST_URL", "http://10.65.18.9:9000")

# default s3 indicator write bucket
S3_DEFAULT_INDICATOR_BUCKET = os.getenv("WM_S3_DEFAULT_INDICATOR_BUCKET", "new-indicators")

# default model s3 write bucket
S3_DEFAULT_MODEL_BUCKET = os.getenv("WM_S3_DEFAULT_MODEL_BUCKET", "new-models")

# default base-image
BASE_IMAGE = os.getenv(
    "WM_DATA_PIPELINE_IMAGE", "docker.uncharted.software/worldmodeler/wm-data-pipeline:latest"
)

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

LAT_LONG_COLUMNS = ["lat", "lng"]

# Pandas internally uses ns for timestamps and cannot represent time larger than int64.max nanoseconds
# This is the max number of ms we can represent
MAX_TIMESTAMP = np.iinfo(np.int64).max / 1_000_000


@task(log_stdout=True)
def read_data(source, data_paths) -> Tuple[dd.DataFrame, int]:
    df = None
    # if source is from s3 bucket
    # used for testing
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
            if re.match(".*_str(.[0-9]+)?.parquet.gzip$", path):
                string_files.append(path)
            else:
                numeric_files.append(path)

        # Note: dask read_parquet doesn't work for gzip files. So here is the work around using pandas read_parquet
        # Read each parquet file in separately, and ensure that all columns match before joining together
        delayed_dfs = [delayed(pd.read_parquet)(path) for path in numeric_files]
        dfs = [dd.from_delayed(d) for d in delayed_dfs]

        if len(dfs) == 0:
            raise FAIL("No numeric parquet files")
        elif len(dfs) == 1:
            df = dfs[0]
        else:
            # Add missing columns into all dataframes and ensure all are strings
            extra_cols = [set(d.columns.to_list()) - REQUIRED_COLS for d in dfs]
            all_extra_cols = set.union(*extra_cols)

            df_col_types = []
            for i in range(len(dfs)):
                # Add missing columns
                cols_to_add = list(all_extra_cols - extra_cols[i])
                for col in cols_to_add:
                    dfs[i][col] = ""
                    dfs[i] = dfs[i].astype({col: "str"})

                # Force the new columns as well as 'feature' to type "str"
                type_dict = {col: "str" for col in cols_to_add + ["feature"]}
                dfs[i] = dfs[i].astype(type_dict)

                # Create a mapping of REGION_LEVELS to their actual types
                df_col_types.append({col: dfs[i][col].dtype.kind for col in REGION_LEVELS})

            # REGION_LEVELS types must match, if they do not, fill with "None" and re-type to "str"
            cols_to_retype = []
            for col in REGION_LEVELS:
                unique_types = set([type_dict[col] for type_dict in df_col_types])
                if len(unique_types) > 1:
                    print(f"Column {col} has types {unique_types}")
                    cols_to_retype.append(col)

            for i in range(len(dfs)):
                dfs[i][cols_to_retype] = (
                    dfs[i][cols_to_retype].fillna(value="None", axis=1).astype("str")
                )

            df = dd.concat(dfs, ignore_unknown_divisions=True).repartition(npartitions=12)

    print("Data types")
    print(df.dtypes)

    print("Top N-rows")
    print(df.head())

    # Remove lat/lng columns if they are null
    ll_df = df[LAT_LONG_COLUMNS]
    null_cols = get_null_or_empty_cols(ll_df)
    if len(set(LAT_LONG_COLUMNS) & null_cols) > 0:
        print("No lat/long data. Dropping columns.")
        df = df.drop(columns=LAT_LONG_COLUMNS)
    else:
        df["lat"] = dd.to_numeric(df["lat"], errors="coerce")
        df["lng"] = dd.to_numeric(df["lng"], errors="coerce")
        df = df.astype({"lat": "float64", "lng": "float64"})

    num_rows = len(df.index)
    print(f"Read {num_rows} rows of data")
    if num_rows == 0:
        raise FAIL("DataFrame has no rows")

    # Ensure types
    df = df.astype({"value": "float64"})
    return (df, num_rows)


def get_null_or_empty_cols(df):
    null_cols = set(df.columns[df.isnull().all()])
    empty_cols = set(df.columns[df.eq("").all()])
    return null_cols | empty_cols


@task(log_stdout=True)
def configure_pipeline(
    df, dest, indicator_bucket, model_bucket, compute_tiles, is_indicator
) -> Tuple[dict, bool, bool, bool, bool]:
    all_cols = df.columns.to_list()
    compute_tiles = compute_tiles and set(LAT_LONG_COLUMNS).issubset(all_cols)

    if is_indicator:
        dest["bucket"] = indicator_bucket
        compute_monthly = True
        compute_annual = True
        compute_summary = False
    else:
        dest["bucket"] = model_bucket
        compute_monthly = True
        compute_annual = True
        compute_summary = True

    return (
        dest,
        compute_monthly,
        compute_annual,
        compute_summary,
        compute_tiles,
    )


@task(log_stdout=True)
def save_raw_data(df, dest, time_res, model_id, run_id, raw_count_threshold):
    raw_df = df.copy()
    output_columns = raw_df.columns.drop("feature")

    raw_df = raw_df.groupby(["feature"]).apply(
        lambda x: raw_data_to_csv(
            x,
            dest,
            model_id,
            run_id,
            time_res,
            int(raw_count_threshold),
            output_columns,
            WRITE_TYPES[DEST_TYPE],
        ),
        meta=(None, "int"),
    )

    rows_per_feature = raw_df.compute().to_json(orient="index")

    return rows_per_feature


@task(log_stdout=True)
def validate_and_fix(df, weight_column, fill_timestamp) -> Tuple[dd.DataFrame, str, int, int, int]:
    # Drop a column if all values are null
    exclude_columns = set(["timestamp", "lat", "lng", "feature", "value"])
    null_cols = get_null_or_empty_cols(df)
    cols_to_drop = list(null_cols - exclude_columns)
    df = df.drop(columns=cols_to_drop)

    # Remove infinities because they cause problems in some aggregation types (e.g. mean)
    df = df.replace({"value": [np.inf, -np.inf]}, np.nan)

    # In the remaining columns, fill all null values with "None"
    # TODO: When adding support for different qualifier roles, we will need to fill numeric roles with something else
    remaining_columns = list(set(df.columns.to_list()) - exclude_columns - null_cols)
    df[remaining_columns] = df[remaining_columns].fillna(value="None", axis=1).astype("str")

    # Remove characters that Minio can't handle
    for col in REGION_LEVELS:
        if col not in cols_to_drop:
            df[col] = df[col].str.replace("//", "")

    if weight_column not in df.columns.to_list():
        weight_column = ""  # rest of the checks in the script will use this
    elif weight_column != "":
        df[weight_column] = dd.to_numeric(df[weight_column], errors="coerce")
        df = df.fillna(value={weight_column: 0}).astype({weight_column: "float64"})

    # Fill missing timestamp values (0 by default)
    num_missing_ts = int(df["timestamp"].isna().sum().compute().item())
    df["timestamp"] = df["timestamp"].fillna(value=fill_timestamp)

    # Remove extreme timestamps
    num_invalid_ts = int((df["timestamp"] >= MAX_TIMESTAMP).sum().compute().item())
    df = df[df["timestamp"] < MAX_TIMESTAMP]

    num_missing_val = int(df["value"].isna().sum().compute().item())
    return (df, weight_column, num_missing_ts, num_invalid_ts, num_missing_val)


@task(log_stdout=True, skip_on_upstream_skip=False)
def temporal_aggregation(df, time_res, should_run, weight_column):
    if should_run is False:
        raise SKIP(f"Aggregating for resolution {time_res} was not requested")
    return run_temporal_aggregation(df, time_res, weight_column)


@task(log_stdout=True)
def compute_timeseries_as_csv(
    df, dest, time_res, model_id, run_id, qualifier_map, qualifer_columns, weight_column
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

        aggregates_to_compute = {"t_sum": timeseries_aggs, "t_mean": timeseries_aggs}
        if weight_column != "":
            timeseries_df["_weighted_sum"] = timeseries_df["t_sum"] * timeseries_df[weight_column]
            timeseries_df["_weighted_mean"] = timeseries_df["t_mean"] * timeseries_df[weight_column]
            timeseries_df["_weighted_wavg"] = timeseries_df["t_wavg"] * timeseries_df[weight_column]

            aggregates_to_compute["_weighted_sum"] = ["sum"]
            aggregates_to_compute["_weighted_mean"] = ["sum"]
            aggregates_to_compute["_weighted_wavg"] = ["sum"]
            aggregates_to_compute[weight_column] = ["sum"]
            aggregates_to_compute["t_wavg"] = timeseries_aggs

            timeseries_lookup.update(
                {
                    # wavg of temporal
                    ("_weighted_sum", "sum"): "_weighted_sum",
                    ("_weighted_mean", "sum"): "_weighted_mean",
                    ("_weighted_wavg", "sum"): "_weighted_wavg",
                    (weight_column, "sum"): "_weight_sum",
                    # spatial agg of wavg
                    ("t_wavg", "min"): "s_min_t_wavg",
                    ("t_wavg", "max"): "s_max_t_wavg",
                    ("t_wavg", "sum"): "s_sum_t_wavg",
                    ("t_wavg", "mean"): "s_mean_t_wavg",
                }
            )
            timeseries_agg_columns.extend(
                [
                    "s_min_t_wavg",
                    "s_max_t_wavg",
                    "s_sum_t_wavg",
                    "s_mean_t_wavg",
                    # next 3 columns are computed below
                    "s_wavg_t_sum",
                    "s_wavg_t_mean",
                    "s_wavg_t_wavg",
                ]
            )

        timeseries_df = timeseries_df.groupby(["feature", "timestamp"] + qualifier_col).agg(
            aggregates_to_compute
        )
        timeseries_df.columns = timeseries_df.columns.to_flat_index()
        timeseries_df = timeseries_df.rename(columns=timeseries_lookup).reset_index()

        if weight_column != "":
            timeseries_df["s_wavg_t_sum"] = (
                timeseries_df["_weighted_sum"] / timeseries_df["_weight_sum"]
            )
            timeseries_df["s_wavg_t_mean"] = (
                timeseries_df["_weighted_mean"] / timeseries_df["_weight_sum"]
            )
            timeseries_df["s_wavg_t_wavg"] = (
                timeseries_df["_weighted_wavg"] / timeseries_df["_weight_sum"]
            )
            timeseries_df = timeseries_df.drop(
                columns=["_weighted_sum", "_weighted_mean", "_weighted_wavg", "_weight_sum"]
            )

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
            meta=(None, "int"),
        )

        timeseries_pdf = timeseries_df.compute()
        if len(qualifier_col) == 0:
            timeseries_size = timeseries_pdf.to_json(orient="index")

    return timeseries_size


@task(log_stdout=True)
def compute_regional_aggregation_to_csv(
    input_df, dest, time_res, model_id, run_id, qualifier_map, qualifer_columns, weight_column
):
    qualifier_cols = [*qualifer_columns, []]  # [] is the default case of ignoring qualifiers

    # Copy input df so that original df doesn't get mutated
    df = input_df.copy()
    # Ranme columns
    df.columns = df.columns.str.replace("t_sum", "s_sum_t_sum").str.replace(
        "t_mean", "s_sum_t_mean"
    )
    if weight_column != "":
        df.columns = df.columns.str.replace("t_wavg", "s_sum_t_wavg")
    else:
        df["s_sum_t_wavg"] = 0

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

        aggregates_to_compute = {
            "s_sum_t_sum": ["sum"],
            "s_sum_t_mean": ["sum"],
            "s_sum_t_wavg": ["sum"],
            "s_count": ["sum"],
        }

        rename_lookup = {
            ("s_sum_t_sum", "sum"): "s_sum_t_sum",
            ("s_sum_t_mean", "sum"): "s_sum_t_mean",
            ("s_sum_t_wavg", "sum"): "s_sum_t_wavg",
            ("s_count", "sum"): "s_count",
        }

        select_cols = (
            ["feature", "timestamp", "s_sum_t_sum", "s_sum_t_mean", "s_sum_t_wavg", "s_count"]
            + regions_cols
            + qualifier_col
        )

        if weight_column != "":
            qualifier_df["_weighted_sum"] = (
                qualifier_df["s_sum_t_sum"] * qualifier_df[weight_column]
            )
            qualifier_df["_weighted_mean"] = (
                qualifier_df["s_sum_t_mean"] * qualifier_df[weight_column]
            )
            qualifier_df["_weighted_wavg"] = (
                qualifier_df["s_sum_t_wavg"] * qualifier_df[weight_column]
            )

            aggregates_to_compute["_weighted_sum"] = ["sum"]
            aggregates_to_compute["_weighted_mean"] = ["sum"]
            aggregates_to_compute["_weighted_wavg"] = ["sum"]
            aggregates_to_compute[weight_column] = ["sum"]

            rename_lookup.update(
                {
                    ("_weighted_sum", "sum"): "_weighted_sum",
                    ("_weighted_mean", "sum"): "_weighted_mean",
                    ("_weighted_wavg", "sum"): "_weighted_wavg",
                    (weight_column, "sum"): "_weight_sum",
                }
            )
            select_cols.extend(["_weighted_sum", "_weighted_mean", "_weighted_wavg", weight_column])

        # Region aggregation at the highest admin level
        qualifier_df = (
            qualifier_df[select_cols]
            .groupby(["feature", "timestamp"] + regions_cols + qualifier_col)
            .agg(aggregates_to_compute)
        )

        # Rename columns
        qualifier_df.columns = qualifier_df.columns.to_flat_index()
        qualifier_df = qualifier_df.rename(columns=rename_lookup).reset_index()

        # This is done later in write_regional_aggregation_csv and save_regional_qualifiers_to_csv
        # if weight_column != "":
        #     timeseries_df["s_wavg_t_sum"] = timeseries_df["_weighted_sum"] / timeseries_df["_weight_sum"]
        #     timeseries_df["s_wavg_t_mean"] = timeseries_df["_weighted_mean"] / timeseries_df["_weight_sum"]
        #     timeseries_df["s_wavg_t_wavg"] = timeseries_df["_weighted_wavg"] / timeseries_df["_weight_sum"]
        #     timeseries_df = timeseries_df.drop(
        #         columns=["_weighted_sum", "_weighted_mean", "_weighted_wavg", "_weight_sum"]
        #     )

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
                    weight_column,
                    f"regional/{regions_cols[level]}",
                )

            desired_columns = [
                "feature",
                "timestamp",
                "region_id",
                "s_sum_t_sum",
                "s_sum_t_mean",
                "s_sum_t_wavg",
                "s_count",
            ] + qualifier_col
            if weight_column != "":
                desired_columns.extend(
                    ["_weighted_sum", "_weighted_mean", "_weighted_wavg", "_weight_sum"]
                )

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
                        weight_column,
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
                        weight_column,
                        qualifier_map,
                        WRITE_TYPES[DEST_TYPE],
                        region_level=regions_cols[level],
                    ),
                    axis=1,
                    meta=(None, "object"),
                )
                foo.compute()


@task(log_stdout=True)
def get_qualifier_columns(df, weight_col):
    base_cols = REQUIRED_COLS
    all_cols = df.columns.to_list()
    if weight_col in all_cols:
        all_cols.remove(weight_col)
    qualifier_cols = [[col] for col in set(all_cols) - base_cols]

    return qualifier_cols


@task(log_stdout=True)
def compute_regional_timeseries(
    df,
    dest,
    time_res,
    model_id,
    run_id,
    qualifier_map,
    qualifier_columns,
    qualifier_counts,
    qualifier_thresholds,
    weight_column,
):
    max_qualifier_count = qualifier_thresholds["regional_timeseries_count"]
    max_level = qualifier_thresholds["regional_timeseries_max_level"]
    (new_qualifier_map, new_qualifier_columns) = apply_qualifier_count_limit(
        qualifier_map, qualifier_columns, qualifier_counts, max_qualifier_count
    )

    regions_cols = extract_region_columns(df)
    for level, region_level in enumerate(regions_cols):
        # Qualifiers won't be used for admin levels above the threshold
        if level > max_level:
            qualifier_cols = [[]]  # [] is the default case of ignoring qualifiers
        else:
            qualifier_cols = [*new_qualifier_columns, []]

        compute_timeseries_by_region(
            df,
            dest,
            model_id,
            run_id,
            time_res,
            region_level,
            new_qualifier_map,
            qualifier_cols,
            weight_column,
            WRITE_TYPES[DEST_TYPE],
        )


@task(log_stdout=True)
def subtile_aggregation(df, weight_column, should_run):
    if should_run is False:
        raise SKIP("Tiling was not requested")

    # Spatial aggregation to the higest supported precision(subtile z) level

    stile = df.apply(
        lambda x: deg2num(x.lat, x.lng, MAX_SUBTILE_PRECISION), axis=1, meta=(None, "object")
    )
    subtile_df = df.assign(subtile=stile)

    subset_cols = ["feature", "timestamp", "subtile", "t_sum", "t_mean"]
    aggregates_to_compute = {"t_sum": ["sum", "count"], "t_mean": ["sum", "count"]}

    # Rename columns
    spatial_lookup = {
        ("t_sum", "sum"): "s_sum_t_sum",
        ("t_sum", "count"): "s_count_t_sum",
        ("t_mean", "sum"): "s_sum_t_mean",
        ("t_mean", "count"): "s_count",
    }

    if weight_column != "":
        subtile_df["_weighted_sum"] = subtile_df["t_sum"] * subtile_df[weight_column]
        subtile_df["_weighted_mean"] = subtile_df["t_mean"] * subtile_df[weight_column]
        subtile_df["_weighted_wavg"] = subtile_df["t_wavg"] * subtile_df[weight_column]

        aggregates_to_compute["_weighted_sum"] = ["sum"]
        aggregates_to_compute["_weighted_mean"] = ["sum"]
        aggregates_to_compute["_weighted_wavg"] = ["sum"]
        aggregates_to_compute[weight_column] = ["sum"]
        aggregates_to_compute["t_wavg"] = ["sum", "mean"]  # mean can be caluculated here directly

        spatial_lookup.update(
            {
                # wavg of temporal
                ("_weighted_sum", "sum"): "_weighted_sum",
                ("_weighted_mean", "sum"): "_weighted_mean",
                ("_weighted_wavg", "sum"): "_weighted_wavg",
                (weight_column, "sum"): "_weight_sum",
                # spatial agg of wavg
                ("t_wavg", "sum"): "s_sum_t_wavg",
                ("t_wavg", "mean"): "s_mean_t_wavg",
            }
        )
        subset_cols.extend(
            ["t_wavg", "_weighted_sum", "_weighted_mean", "_weighted_wavg", weight_column]
        )

    subtile_df = (
        subtile_df[subset_cols]
        .groupby(["feature", "timestamp", "subtile"])
        .agg(aggregates_to_compute)
    )
    subtile_df.columns = subtile_df.columns.to_flat_index()
    subtile_df = (
        subtile_df.rename(columns=spatial_lookup).drop(columns=["s_count_t_sum"]).reset_index()
    )

    if weight_column != "":
        subtile_df["s_wavg_t_sum"] = subtile_df["_weighted_sum"] / subtile_df["_weight_sum"]
        subtile_df["s_wavg_t_mean"] = subtile_df["_weighted_mean"] / subtile_df["_weight_sum"]
        subtile_df["s_wavg_t_wavg"] = subtile_df["_weighted_wavg"] / subtile_df["_weight_sum"]
        subtile_df = subtile_df.drop(
            columns=["_weighted_sum", "_weighted_mean", "_weighted_wavg", "_weight_sum"]
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
            lambda x: save_tile(  # To test use: save_tile_to_csv
                to_proto(x),  # To test use: to_tile_csv
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
def compute_stats(df, dest, time_res, model_id, run_id):
    compute_subtile_stats(
        df,
        dest,
        model_id,
        run_id,
        time_res,
        MIN_SUBTILE_PRECISION,
        WRITE_TYPES[DEST_TYPE],
    )


def assist_compute_stats(df, dest, time_res, model_id, run_id, weight_column, filename):
    # Compute mean and get new dataframe with mean columns added
    stats_df = df.assign(
        s_mean_t_sum=df["s_sum_t_sum"] / df["s_count"],
        s_mean_t_mean=df["s_sum_t_mean"] / df["s_count"],
        s_mean_t_wavg=df["s_sum_t_wavg"] / df["s_count"],
    )
    if weight_column != "":
        stats_df["s_wavg_t_sum"] = stats_df["_weighted_sum"] / stats_df["_weight_sum"]
        stats_df["s_wavg_t_mean"] = stats_df["_weighted_mean"] / stats_df["_weight_sum"]
        stats_df["s_wavg_t_wavg"] = stats_df["_weighted_wavg"] / stats_df["_weight_sum"]

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

    aggregations_to_compute = {
        "s_sum_t_sum": stats_aggs,
        "s_mean_t_sum": stats_aggs,
        "s_sum_t_mean": stats_aggs,
        "s_mean_t_mean": stats_aggs,
    }

    if weight_column != "":
        stats_lookup.update(
            {
                ("s_wavg_t_mean", "min"): "min_s_wavg_t_mean",
                ("s_wavg_t_mean", "max"): "max_s_wavg_t_mean",
                ("s_wavg_t_sum", "min"): "min_s_wavg_t_sum",
                ("s_wavg_t_sum", "max"): "max_s_wavg_t_sum",
                ("s_sum_t_wavg", "min"): "min_s_sum_t_wavg",
                ("s_sum_t_wavg", "max"): "max_s_sum_t_wavg",
                ("s_mean_t_wavg", "min"): "min_s_mean_t_wavg",
                ("s_mean_t_wavg", "max"): "max_s_mean_t_wavg",
                ("s_wavg_t_wavg", "min"): "min_s_wavg_t_wavg",
                ("s_wavg_t_wavg", "max"): "max_s_wavg_t_wavg",
            }
        )
        stats_agg_columns.extend(
            [
                "min_s_wavg_t_sum",
                "max_s_wavg_t_sum",
                "min_s_wavg_t_mean",
                "max_s_wavg_t_mean",
                "min_s_sum_t_wavg",
                "max_s_sum_t_wavg",
                "min_s_mean_t_wavg",
                "max_s_mean_t_wavg",
                "min_s_wavg_t_wavg",
                "max_s_wavg_t_wavg",
            ]
        )
        aggregations_to_compute.update(
            {
                "s_wavg_t_sum": stats_aggs,
                "s_wavg_t_mean": stats_aggs,
                "s_sum_t_wavg": stats_aggs,
                "s_mean_t_wavg": stats_aggs,
                "s_wavg_t_wavg": stats_aggs,
            }
        )

    stats_df = stats_df.groupby(["feature"]).agg(aggregations_to_compute)
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
def compute_output_summary(df, weight_column):
    timeseries_df = df.copy()

    # Timeseries aggregation
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

    aggregations_to_compute = {"t_sum": timeseries_aggs, "t_mean": timeseries_aggs}
    if weight_column != "":
        timeseries_df["_weighted_sum"] = timeseries_df["t_sum"] * timeseries_df[weight_column]
        timeseries_df["_weighted_mean"] = timeseries_df["t_mean"] * timeseries_df[weight_column]
        timeseries_df["_weighted_wavg"] = timeseries_df["t_wavg"] * timeseries_df[weight_column]

        aggregations_to_compute["_weighted_sum"] = ["sum"]
        aggregations_to_compute["_weighted_mean"] = ["sum"]
        aggregations_to_compute["_weighted_wavg"] = ["sum"]
        aggregations_to_compute[weight_column] = ["sum"]
        aggregations_to_compute["t_wavg"] = timeseries_aggs

        timeseries_lookup.update(
            {
                # wavg of temporal
                ("_weighted_sum", "sum"): "_weighted_sum",
                ("_weighted_mean", "sum"): "_weighted_mean",
                ("_weighted_wavg", "sum"): "_weighted_wavg",
                (weight_column, "sum"): "_weight_sum",
                # spatial agg of wavg
                ("t_wavg", "min"): "s_min_t_wavg",
                ("t_wavg", "max"): "s_max_t_wavg",
                ("t_wavg", "sum"): "s_sum_t_wavg",
                ("t_wavg", "mean"): "s_mean_t_wavg",
            }
        )
        timeseries_agg_columns.extend(
            [
                "s_min_t_wavg",
                "s_max_t_wavg",
                "s_sum_t_wavg",
                "s_mean_t_wavg",
                # next 3 columns are computed below
                "s_wavg_t_sum",
                "s_wavg_t_mean",
                "s_wavg_t_wavg",
            ]
        )

    timeseries_df = timeseries_df.groupby(["feature", "timestamp"]).agg(aggregations_to_compute)
    timeseries_df.columns = timeseries_df.columns.to_flat_index()
    timeseries_df = timeseries_df.rename(columns=timeseries_lookup).reset_index()

    if weight_column != "":
        timeseries_df["s_wavg_t_sum"] = (
            timeseries_df["_weighted_sum"] / timeseries_df["_weight_sum"]
        )
        timeseries_df["s_wavg_t_mean"] = (
            timeseries_df["_weighted_mean"] / timeseries_df["_weight_sum"]
        )
        timeseries_df["s_wavg_t_wavg"] = (
            timeseries_df["_weighted_wavg"] / timeseries_df["_weight_sum"]
        )
        timeseries_df = timeseries_df.drop(
            columns=["_weighted_sum", "_weighted_mean", "_weighted_wavg", "_weight_sum"]
        )

    summary = output_values_to_json_array(timeseries_df[["feature"] + timeseries_agg_columns])
    return summary


@task(skip_on_upstream_skip=False, log_stdout=True)
def record_results(
    dest,
    model_id,
    run_id,
    summary_values,
    num_rows,
    rows_per_feature,
    region_columns,
    feature_list,
    raw_count_threshold,
    compute_monthly,
    compute_annual,
    compute_tiles,
    month_ts_size,
    year_ts_size,
    num_missing_ts,
    num_invalid_ts,
    num_missing_val,
    weight_column,
):
    if compute_tiles == True:
        region_columns.append("grid data")

    data = {
        "data_info": {
            "num_rows": num_rows,
            "num_rows_per_feature": json.loads(rows_per_feature),
            "num_missing_ts": num_missing_ts,
            "num_invalid_ts": num_invalid_ts,
            "num_missing_val": num_missing_val,
            "region_levels": region_columns,
            "features": feature_list,
            "raw_count_threshold": int(raw_count_threshold),
            "has_tiles": compute_tiles,
            "has_monthly": compute_monthly,
            "has_annual": compute_annual,
            "has_weights": weight_column != "",
        },
    }
    if summary_values is not None:
        data["output_agg_values"] = summary_values

    if compute_monthly and month_ts_size is not None:
        data["data_info"]["month_timeseries_size"] = json.loads(month_ts_size)

    if compute_annual and year_ts_size is not None:
        data["data_info"]["year_timeseries_size"] = json.loads(year_ts_size)

    results_to_json(
        data,
        dest,
        model_id,
        run_id,
        WRITE_TYPES[DEST_TYPE],
    )


@task(log_stdout=True)
def record_region_lists(df, dest, model_id, run_id) -> Tuple[list, list]:
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
        return feature

    region_cols = extract_region_columns(df)
    if len(region_cols) == 0:
        raise SKIP("No regional information available")

    save_df = df.copy()

    # ["__region_id_0", "__region_id_1", "__region_id_2", "__region_id_3"]
    id_cols = [f"__region_id_{level}" for level in range(len(region_cols))]
    for index, id_col in enumerate(id_cols):
        save_df[id_col] = join_region_columns(save_df, region_cols, index)

    features = (
        save_df[["feature"] + id_cols]
        .groupby(["feature"])
        .apply(
            lambda x: cols_to_lists(x, id_cols, x["feature"].values[0]),
            meta=(None, "str"),
        )
    )

    # As a side effect, return the regions available, and the list of features
    feature_list = features.compute().unique().tolist()
    return region_cols, feature_list


@task(log_stdout=True)
def record_qualifier_lists(df, dest, model_id, run_id, qualifiers, thresholds):
    def save_qualifier_lists(df):
        feature = df["feature"].values[0]
        qualifier_counts = {
            "thresholds": thresholds,
            "counts": {},
        }
        for col in qualifier_columns:
            values = df[col].unique().tolist()
            qualifier_counts["counts"][col] = len(values)

            # Write one list of qualifier values per file
            info_to_json(
                values,
                dest,
                model_id,
                run_id,
                feature,
                f"qualifiers/{col}",
                WRITE_TYPES[DEST_TYPE],
            )

        # Write a file that holds the counts of the above qualifier lists
        info_to_json(
            qualifier_counts,
            dest,
            model_id,
            run_id,
            feature,
            f"qualifier_counts",
            WRITE_TYPES[DEST_TYPE],
        )
        return qualifier_counts["counts"]

    save_df = df.copy()
    qualifier_columns = sum(qualifiers, [])

    counts = (
        save_df[["feature"] + qualifier_columns]
        .groupby(["feature"])
        .apply(lambda x: save_qualifier_lists(x), meta=(None, "object"))
    )
    pdf_counts = counts.compute()
    json_str = pdf_counts.to_json(orient="index")
    return json.loads(json_str)


@task(log_stdout=True)
def apply_qualifier_thresholds(qualifier_map, columns, counts, thresholds) -> Tuple[dict, list]:
    max_count = thresholds["max_count"]
    return apply_qualifier_count_limit(qualifier_map, columns, counts, max_count)


@task(skip_on_upstream_skip=False, log_stdout=True)
def print_flow_metadata(
    model_id,
    run_id,
    data_paths,
    dest,
    compute_monthly,
    compute_annual,
    compute_summary,
    compute_tiles,
):
    # Print out useful/debugging information
    print("===== Flow Summary =====")
    print(f"Run ID: {run_id}")
    print(f"Model ID: {model_id}")
    for path in data_paths:
        print(f"Data paths: {path}")
    print(f"Destination URL: {dest['endpoint_url']}")
    print(f"Destination bucket: {dest['bucket']}")
    print(f"Compute monthly: {compute_monthly}")
    print(f"Compute yearly: {compute_annual}")
    print(f"Compute summary: {compute_summary}")
    print(f"Compute tiles: {compute_tiles}")


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
        base_image=BASE_IMAGE,
        image_name=image_name,
        local_image=True,
        stored_as_script=True,
        path="/wm_data_pipeline/flows/data_pipeline.py",
        ignore_healthchecks=True,
    )

    # Parameters
    model_id = Parameter("model_id", default="geo-test-data")
    run_id = Parameter("run_id", default="test-run")
    data_paths = Parameter("data_paths", default=["s3://test/geo-test-data.parquet"])
    compute_tiles = Parameter("compute_tiles", default=True)
    raw_count_threshold = Parameter("raw_count_threshold", default=10000)
    is_indicator = Parameter("is_indicator", default=False)
    qualifier_map = Parameter("qualifier_map", default={})
    indicator_bucket = Parameter("indicator_bucket", default=S3_DEFAULT_INDICATOR_BUCKET)
    model_bucket = Parameter("model_bucket", default=S3_DEFAULT_MODEL_BUCKET)
    fill_timestamp = Parameter("fill_timestamp", default=0)
    weight_column = Parameter("weight_column", default="")
    # The thresholds are values that the pipeline uses for processing qualifiers
    # It tells the user what sort of data they can expect to be available
    # For ex, no regional_timeseries for qualifier with more than 100 values
    qualifier_thresholds = Parameter(
        "qualifier_thresholds",
        default={
            "max_count": 10000,
            "regional_timeseries_count": 100,
            "regional_timeseries_max_level": 1,
        },
    )

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

    (raw_df, num_rows) = read_data(source, data_paths)

    # ==== Set parameters that determine which tasks should run based on the type of data we're ingesting ====
    (dest, compute_monthly, compute_annual, compute_summary, compute_tiles) = configure_pipeline(
        raw_df, dest, indicator_bucket, model_bucket, compute_tiles, is_indicator
    )

    # ==== Save raw data =====
    rows_per_feature = save_raw_data(raw_df, dest, "raw", model_id, run_id, raw_count_threshold)

    (df, weight_column, num_missing_ts, num_invalid_ts, num_missing_val) = validate_and_fix(
        raw_df, weight_column, fill_timestamp
    )

    qualifier_columns = get_qualifier_columns(df, weight_column)

    # ==== Compute lists of all gadm regions and qualifier values =====
    (region_columns, feature_list) = record_region_lists(df, dest, model_id, run_id)
    qualifier_counts = record_qualifier_lists(
        df, dest, model_id, run_id, qualifier_columns, qualifier_thresholds
    )
    (qualifier_map, qualifier_columns) = apply_qualifier_thresholds(
        qualifier_map, qualifier_columns, qualifier_counts, qualifier_thresholds
    )

    # ==== Run aggregations based on monthly time resolution =====
    monthly_data = temporal_aggregation(df, "month", compute_monthly, weight_column)
    month_ts_size = compute_timeseries_as_csv(
        monthly_data,
        dest,
        "month",
        model_id,
        run_id,
        qualifier_map,
        qualifier_columns,
        weight_column,
    )
    compute_regional_timeseries(
        monthly_data,
        dest,
        "month",
        model_id,
        run_id,
        qualifier_map,
        qualifier_columns,
        qualifier_counts,
        qualifier_thresholds,
        weight_column,
    )
    monthly_csv_regional_df = compute_regional_aggregation_to_csv(
        monthly_data,
        dest,
        "month",
        model_id,
        run_id,
        qualifier_map,
        qualifier_columns,
        weight_column,
    )

    monthly_spatial_data = subtile_aggregation(
        monthly_data, weight_column, compute_tiles, upstream_tasks=[month_ts_size]
    )
    month_stats_done = compute_stats(monthly_spatial_data, dest, "month", model_id, run_id)
    month_done = compute_tiling(
        monthly_spatial_data, dest, "month", model_id, run_id, upstream_tasks=[month_stats_done]
    )

    # ==== Run aggregations based on annual time resolution =====
    annual_data = temporal_aggregation(
        df,
        "year",
        compute_annual,
        weight_column,
        upstream_tasks=[monthly_csv_regional_df, month_done],
    )
    year_ts_size = compute_timeseries_as_csv(
        annual_data, dest, "year", model_id, run_id, qualifier_map, qualifier_columns, weight_column
    )
    compute_regional_timeseries(
        annual_data,
        dest,
        "year",
        model_id,
        run_id,
        qualifier_map,
        qualifier_columns,
        qualifier_counts,
        qualifier_thresholds,
        weight_column,
    )
    annual_csv_regional_df = compute_regional_aggregation_to_csv(
        annual_data, dest, "year", model_id, run_id, qualifier_map, qualifier_columns, weight_column
    )

    annual_spatial_data = subtile_aggregation(
        annual_data, weight_column, compute_tiles, upstream_tasks=[year_ts_size]
    )
    year_stats_done = compute_stats(annual_spatial_data, dest, "year", model_id, run_id)
    year_done = compute_tiling(
        annual_spatial_data, dest, "year", model_id, run_id, upstream_tasks=[year_stats_done]
    )

    # ==== Generate a single aggregate value per feature =====
    summary_data = temporal_aggregation(
        df,
        "all",
        compute_summary,
        weight_column,
        upstream_tasks=[year_done, year_ts_size, annual_csv_regional_df],
    )
    summary_values = compute_output_summary(summary_data, weight_column)

    # ==== Record the results in Minio =====
    record_results(
        dest,
        model_id,
        run_id,
        summary_values,
        num_rows,
        rows_per_feature,
        region_columns,
        feature_list,
        raw_count_threshold,
        compute_monthly,
        compute_annual,
        compute_tiles,
        month_ts_size,
        year_ts_size,
        num_missing_ts,
        num_invalid_ts,
        num_missing_val,
        weight_column,
    )

    # === Print out useful information about the flow metadata =====
    print_flow_metadata(
        model_id,
        run_id,
        data_paths,
        dest,
        compute_monthly,
        compute_annual,
        compute_summary,
        compute_tiles,
        upstream_tasks=[summary_values],
    )

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
        # flow.run(
        #     parameters=dict(  # Maxhop
        #         compute_tiles=True,
        #         model_id="maxhop-v0.2",
        #         run_id="4675d89d-904c-466f-a588-354c047ecf72",
        #         data_paths=[
        #             "https://jataware-world-modelers.s3.amazonaws.com/dmc_results/4675d89d-904c-466f-a588-354c047ecf72/4675d89d-904c-466f-a588-354c047ecf72_maxhop-v0.2.parquet.gzip"
        #         ],
        #     )
        # )
        # flow.run( # Test qualifiers
        #     parameters=dict(
        #         is_indicator=True,
        #         qualifier_map={
        #             "fatalities": [
        #                 "event_type",
        #                 "sub_event_type",
        #                 "source_scale",
        #                 "country_non_primary",
        #                 "admin1_non_primary",
        #             ]
        #         },
        #         model_id="_qualifier-test",
        #         run_id="indicator",
        #         data_paths=["s3://test/_qualifier-test.bin"],
        #     )
        # )
        # flow.run( # Test combining multiple parquet files with different columns
        #     parameters={
        #         "compute_tiles": True,
        #         "data_paths": [
        #             "https://jataware-world-modelers.s3.amazonaws.com/dmc_results_dev/9d7db850-0abe-486f-8979-b1e9ad2ef6ad/9d7db850-0abe-486f-8979-b1e9ad2ef6ad_7b1ceeb4-95a3-4bfd-b7cd-e2a89391742f.1.parquet.gzip",
        #             "https://jataware-world-modelers.s3.amazonaws.com/dmc_results_dev/9d7db850-0abe-486f-8979-b1e9ad2ef6ad/9d7db850-0abe-486f-8979-b1e9ad2ef6ad_7b1ceeb4-95a3-4bfd-b7cd-e2a89391742f.2.parquet.gzip"
        #             ],
        #         "is_indicator": False,
        #         "model_id": "7b1ceeb4-95a3-4bfd-b7cd-e2a89391742f",
        #         "qualifier_map": {
        #             "yield_loss_risk": ["longitude", "latitude", "time"],
        #             "harvested_area_at_risk": ["NameCrop", "NameIrrigation", "NameCategory"]
        #         },
        #         "qualifier_thresholds": {
        #             "max_count": 10000,
        #             "regional_timeseries_count": 100,
        #             "regional_timeseries_max_level": 1
        #         },
        #         "run_id": "9d7db850-0abe-486f-8979-b1e9ad2ef6ad"
        #     }
        # )
        # flow.run(  # Test combining multiple parquet files with different columns
        #     parameters={
        #         "compute_tiles": True,
        #         "data_paths": [
        #             "https://jataware-world-modelers.s3.amazonaws.com/dmc_results_dev/f2818712-09f7-49c6-b920-ea21c764d1c7/f2818712-09f7-49c6-b920-ea21c764d1c7_84fd427f-3a7d-473f-aa25-0c0a150ca216.3.parquet.gzip",
        #             "https://jataware-world-modelers.s3.amazonaws.com/dmc_results_dev/f2818712-09f7-49c6-b920-ea21c764d1c7/f2818712-09f7-49c6-b920-ea21c764d1c7_84fd427f-3a7d-473f-aa25-0c0a150ca216.2.parquet.gzip",
        #             "https://jataware-world-modelers.s3.amazonaws.com/dmc_results_dev/f2818712-09f7-49c6-b920-ea21c764d1c7/f2818712-09f7-49c6-b920-ea21c764d1c7_84fd427f-3a7d-473f-aa25-0c0a150ca216.1.parquet.gzip",
        #         ],
        #         "is_indicator": False,
        #         "model_id": "84fd427f-3a7d-473f-aa25-0c0a150ca216",
        #         "qualifier_map": {
        #             "export [kcal]": [],
        #             "import [kcal]": [],
        #             "supply [kcal]": [],
        #             "Production [mt]": ["Year"],
        #             "Consumption [mt]": ["Year"],
        #             "Ending_stock [mt]": ["Year"],
        #             "production [kcal]": [],
        #             "World market_price [US$/mt]": ["Year"],
        #             "export per capita [kcal pc]": [],
        #             "import per capita [kcal pc]": [],
        #             "supply per capita [kcal pc]": [],
        #             "production change per capita [kcal pc]": [],
        #         },
        #         "qualifier_thresholds": {
        #             "max_count": 10000,
        #             "regional_timeseries_count": 100,
        #             "regional_timeseries_max_level": 1,
        #         },
        #         "run_id": "f2818712-09f7-49c6-b920-ea21c764d1c7",
        #     }
        # )
        # flow.run(
        #     parameters=dict(  # Invalid timestamps
        #         compute_tiles=True,
        #         model_id="087c3e5a-cd3d-4ebc-bc5e-e13a4654005c",
        #         run_id="9e1100d5-06e8-48b6-baea-56b3b820f82d",
        #         data_paths=[
        #             "https://jataware-world-modelers.s3.amazonaws.com/dmc_results_dev/9e1100d5-06e8-48b6-baea-56b3b820f82d/9e1100d5-06e8-48b6-baea-56b3b820f82d_087c3e5a-cd3d-4ebc-bc5e-e13a4654005c.1.parquet.gzip"
        #         ],
        #     )
        # )
        # flow.run( # LPJmL
        #     parameters=dict(
        #         is_indicator=True,
        #         qualifier_map={},
        #         model_id="_hierarchy-test",
        #         run_id="indicator",
        #         data_paths=["s3://test/_hierarchy-test.bin"],
        #     )
        # )
        # flow.run(  # For testing weight column
        #     parameters=dict(  # Weights small
        #         compute_tiles=True,
        #         qualifier_map={"sam_rate": ["qual_1"], "gam_rate": ["qual_1"]},
        #         weight_column="weights",
        #         model_id="_weight-test-small",
        #         run_id="indicator",
        #         data_paths=["s3://test/weight-col.bin"],
        #     )
        # )
        # flow.run(
        #     parameters=dict(  # Weights
        #         compute_tiles=True,
        #         is_indicator=True,
        #         qualifier_map={
        #             "Surveyed Area": ["Locust Presence", "Control Pesticide Name"],
        #             "Control Area Treated": ["Control Pesticide Name"],
        #             "Estimated Control Kill (Mortality Rate)": ["Control Pesticide Name"],
        #         },
        #         weight_column="Locust Breeding",
        #         model_id="_weight-test-1",
        #         run_id="indicator",
        #         data_paths=[
        #             "https://jataware-world-modelers.s3.amazonaws.com/dev/indicators/39f7959d-a63e-4db4-a54d-24c66184cf82/39f7959d-a63e-4db4-a54d-24c66184cf82.parquet.gzip"
        #         ],
        #     )
        # )
        # flow.run(
        #     parameters=dict(  # Real weights
        #         compute_tiles=True,
        #         is_indicator=False,
        #         qualifier_map={
        #             "HWAM_AVE": ["year", "mgn", "season"],
        #             "production": ["year", "mgn", "season"],
        #             "crop_failure_area": ["year", "mgn", "season"],
        #             "TOTAL_NITROGEN_APPLIED": ["year", "mgn", "season"]
        #         },
        #         weight_column="HAREA_TOT",
        #         model_id="2af38a88-aa34-4f4a-94f6-a3e1e6630833",
        #         run_id="eba6ca6b-8c7f-44d1-b008-4349491cabf5",
        #         data_paths=[
        #             "https://jataware-world-modelers.s3.amazonaws.com/dmc_results_dev/eba6ca6b-8c7f-44d1-b008-4349491cabf5/eba6ca6b-8c7f-44d1-b008-4349491cabf5_2af38a88-aa34-4f4a-94f6-a3e1e6630833.1.parquet.gzip"
        #         ],
        #     )
        # )
        flow.run(
            parameters=dict(
                compute_tiles=True,
                is_indicator=False,
                model_id="2281e058-d521-4180-8216-54832700cedd",
                run_id="22045d57-aa6a-4df6-a11d-793225878dab",
                data_paths=[
                    "https://jataware-world-modelers.s3.amazonaws.com/dmc_results_dev/22045d57-aa6a-4df6-a11d-793225878dab/22045d57-aa6a-4df6-a11d-793225878dab_2281e058-d521-4180-8216-54832700cedd.1.parquet.gzip"
                ],
                fill_timestamp=0,
                qualifier_map={
                    "max": ["Date", "camp"],
                    "min": ["Date", "camp"],
                    "data": ["Date", "camp"],
                    "mean": ["Date", "camp"],
                    "error": ["Date", "camp"],
                    "median": ["Date", "camp"],
                },
            )
        )
