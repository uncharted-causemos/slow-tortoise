import datetime
from typing import Tuple
import pandas as pd
import dask.dataframe as dd
import numpy as np
import math
import boto3
import json
import sys
import os
import pathlib
import base64

# Bit of a WTF here, but it is well considered.  Dask will serialize the tiles_pb2.Task *class* since it is passed
# to workers within a lambda that calls to_proto.  The problem is that pickling a class object can result in the
# parent *module* object being pickled depending on how its imported, and according to the pickling spec, module
# objects can't be pickled.  This manifests itself as an error on a Dask worker indicating that it can't serialize the
# tiles_pb2 module.  To get around this, we need to import tiles_pb2 module directly, instead of through the flow package,
# which means we need to add the parent directory to the sys path.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
import tiles_pb2

REGION_LEVELS = ["country", "admin1", "admin2", "admin3"]
REQUIRED_COLS = {
    "timestamp",
    "country",
    "admin1",
    "admin2",
    "admin3",
    "lat",
    "lng",
    "feature",
    "value",
}

DEFAULT_PARTITIONS = 8


# Run temporal aggregation on given provided dataframe
def run_temporal_aggregation(df, time_res, weight_column):
    print(
        f"\nrun temporal aggregation dataframe length={len(df.index)},"
        f" npartitions={df.npartitions}\n"
    )
    columns = df.columns.tolist()  # includes qualifier columns
    columns.remove("value")

    # Monthly temporal aggregation (compute for both sum and mean)
    t = dd.to_datetime(df["timestamp"], unit="ms", errors="coerce").apply(
        lambda x: to_normalized_time(x, time_res), meta=(None, "int64[pyarrow]")
    )
    temporal_df = df.assign(timestamp=t)
    # Note: It's not clear why dask is doing this but df.assign(timestamp=t) changes the timestamp column type from int64 to float64 with series t in int64 in some cases
    # (e.g. with "https://jataware-world-modelers.s3.amazonaws.com/transition/datasets/f90318f2-0ac1-4384-ad3b-98f52fff1543/f90318f2-0ac1-4384-ad3b-98f52fff1543.parquet.gzip").
    # TODO: Double check if the issue still exists. Since we've migrated to pyarrow types, the issue may no longer exist anymore
    # We need to change the column type back to int64.
    temporal_df["timestamp"] = temporal_df["timestamp"].astype("int64[pyarrow]")

    aggs = {"value": ["sum", "mean"]}
    rename_map = {
        ("value", "sum"): "t_sum",
        ("value", "mean"): "t_mean",
    }
    # A few temporary columns
    values_col = "_weighted_value"
    weights_col = "_weight_sum"

    if weight_column != "" and weight_column in columns:
        temporal_df[values_col] = temporal_df["value"] * temporal_df[weight_column]

        columns.remove(weight_column)
        aggs[values_col] = ["sum"]
        aggs[weight_column] = ["sum", "mean"]
        rename_map.update(
            {
                (values_col, "sum"): values_col,
                (weight_column, "sum"): weights_col,
                (weight_column, "mean"): weight_column,
            }
        )

    temporal_df = temporal_df.groupby(columns).agg(aggs)
    temporal_df.columns = temporal_df.columns.to_flat_index()
    temporal_df = temporal_df.rename(columns=rename_map).reset_index()

    if weight_column != "":
        temporal_df["t_wavg"] = temporal_df[values_col] / temporal_df[weights_col]
        temporal_df = temporal_df.drop(columns=[values_col, weights_col])

    print(
        f"\nrun temporal aggregation dataframe!! length={len(temporal_df.index)},"
        f" npartitions={temporal_df.npartitions}\n"
    )
    return temporal_df


# returns a look uptable for renaming temporal aggregatd columns
# For example, with temporal agg column `t_sum`, and spatial_aggs, ['sum', 'mean],
# It returns { ("t_sum", "sum"): "s_sum_t_sum", ("t_sum", "mean"): "s_mean_t_sum" }
def create_spatial_agg_rename_lookup(temporal_agg_col, spatial_aggs):
    lookup = {}
    for agg in spatial_aggs:
        col = (temporal_agg_col, agg)
        renamed = f"s_{agg}_{temporal_agg_col}"
        lookup[col] = renamed
    return lookup


# Run spatial aggregation with given spatial_aggs grouped by groupby columns on temporally aggregated dataframe
def run_spatial_aggregation(df, groupby, spatial_aggs, weight_column):
    spatial_aggs = [
        agg for agg in spatial_aggs if agg != "count"
    ]  # remove `count` agg if it exists since it's handled below

    rename_lookup = {}
    rename_lookup.update(create_spatial_agg_rename_lookup("t_sum", spatial_aggs))
    rename_lookup.update(create_spatial_agg_rename_lookup("t_mean", spatial_aggs))
    rename_lookup.update({("t_mean", "count"): "s_count"})

    columns_to_agg = {"t_sum": spatial_aggs, "t_mean": spatial_aggs + ["count"]}
    if weight_column != "":
        df = df.assign(
            _weighted_t_sum=df["t_sum"] * df[weight_column],
            _weighted_t_mean=df["t_mean"] * df[weight_column],
            _weighted_t_wavg=df["t_wavg"] * df[weight_column],
        )

        columns_to_agg["_weighted_t_sum"] = ["sum"]
        columns_to_agg["_weighted_t_mean"] = ["sum"]
        columns_to_agg["_weighted_t_wavg"] = ["sum"]
        columns_to_agg[weight_column] = ["sum"]
        columns_to_agg["t_wavg"] = spatial_aggs

        rename_lookup.update(
            {
                ("_weighted_t_sum", "sum"): "s_wsum_t_sum",
                ("_weighted_t_mean", "sum"): "s_wsum_t_mean",
                ("_weighted_t_wavg", "sum"): "s_wsum_t_wavg",
                (weight_column, "sum"): "s_weight",
            }
        )
        # spatial agg of wavg
        rename_lookup.update(create_spatial_agg_rename_lookup("t_wavg", spatial_aggs))

    df = df.groupby(groupby).agg(columns_to_agg)
    df.columns = df.columns.to_flat_index()
    df = df.rename(columns=rename_lookup).reset_index()

    agg_columns = list(rename_lookup.values())

    if weight_column != "":
        # Add s_wavg cols
        df["s_wavg_t_sum"] = df["s_wsum_t_sum"] / df["s_weight"]
        df["s_wavg_t_mean"] = df["s_wsum_t_mean"] / df["s_weight"]
        df["s_wavg_t_wavg"] = df["s_wsum_t_wavg"] / df["s_weight"]
        agg_columns.extend(["s_wavg_t_sum", "s_wavg_t_mean", "s_wavg_t_wavg"])

        # Drop unnecessary columns
        cols_to_drop = ["s_wsum_t_sum", "s_wsum_t_mean", "s_wsum_t_wavg", "s_weight"]
        df = df.drop(columns=cols_to_drop)
        agg_columns = [col for col in agg_columns if col not in cols_to_drop]

    return (df, agg_columns)

def from_str_coord(coord: str) -> tuple[int, int, int]:
    return tuple(int(el) for el in coord.split("/"))

def to_str_coord(coord: tuple[int, int, int]) -> str:
    return f'{coord[0]}/{coord[1]}/{coord[2]}' # z/x/y

def to_tile_coord(z: int, x: int, y: int) -> str:
    return f'{z}/{x}/{y}'

# More details on tile calculations https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
# Convert lat, long to tile coord
# https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Python
def deg2num(lat_deg: float, lon_deg: float, zoom: int):
    lat_rad = math.radians(lat_deg)
    n = 2.0**zoom
    xtile = int((lon_deg + 180.0) / 360.0 * n)
    ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
    return (zoom, xtile, ytile)

def parent_tile(coord: str, l=1) -> str:
    z, x, y = from_str_coord(coord)
    return to_tile_coord(z - l, math.floor(x / (2**l)), math.floor(y / (2**l)))

# Return the tile that is leveldiff up of given tile. Eg. return (1, 0, 0) for (6, 0, 0) with leveldiff = 5
# The main tile will contain up to 4^leveldiff subtiles with same level
def tile_coord(coord: str, leveldiff=6):
    z, x, y = from_str_coord(coord)
    return to_tile_coord(
        z - leveldiff,
        math.floor(x / math.pow(2, leveldiff)),
        math.floor(y / math.pow(2, leveldiff)),
    )

# project subtile coord into xy coord of the main tile grid (n*n grid where n*n = 4^zdiff)
# https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
def project(subtilecoord, tilecoord):
    z, x, y = from_str_coord(tilecoord)
    sz, sx, sy = from_str_coord(subtilecoord)
    zdiff = sz - z  # zoom level (prececsion) difference

    # Calculate the x and y of the coordinate of the subtile located at the most top left corner of the main tile
    offset_x = math.pow(2, zdiff) * x
    offset_y = math.pow(2, zdiff) * y

    # Project subtile coordinate to n * n (n * n = 4^zdiff) grid coordinate
    binx = sx - offset_x
    biny = sy - offset_y

    # Total number of grid cells
    total_bins = math.pow(4, zdiff)
    max_x_bins = math.sqrt(total_bins)

    bin_index = binx + biny * max_x_bins

    return int(bin_index)


def apply_qualifier_count_limit(qualifier_map, columns, counts, max_count) -> Tuple[dict, list]:
    # Modify qualifier_map to remove qualifiers with too many categories
    new_qualifier_map = {}
    small_qualifiers = set()
    for feature in qualifier_map.keys():
        if feature not in counts:
            continue

        counts_for_feature = counts[feature]
        qualifiers = qualifier_map[feature]
        new_qualifier_map[feature] = [
            q for q in qualifiers if q in counts_for_feature and counts_for_feature[q] <= max_count
        ]
        small_qualifiers.update(new_qualifier_map[feature])

    # Remove any qualifiers from the columns list that are too big for all feature
    # Note: qualifier_columns is in the format [[qualifier1], [qualifier2]]
    qualifier_columns = [col for col in columns if col[0] in small_qualifiers]
    return (new_qualifier_map, qualifier_columns)


# writes to S3 using the boto client
def write_to_s3(body, path, dest):
    # Create s3 client only if it hasn't been created in current worker
    # since initializing the client is expensive. Make sure we only initialize it once per worker
    # Since global variable and the s3 client initialized here seem to be shared across multiple flow runs,
    # we need to also check for the destination url and re-initialize the s3 client when dest url has been changed in different flow run.
    global s3
    if ("s3" not in globals()) or (
        "endpoint_url" in dest and s3.meta.endpoint_url != dest["endpoint_url"]
    ):
        if "endpoint_url" in dest:
            # If custom s3 endpoint url is provided
            s3 = boto3.session.Session().client(
                "s3",
                endpoint_url=dest["endpoint_url"],
                region_name=dest["region_name"],
                aws_access_key_id=dest["key"],
                aws_secret_access_key=dest["secret"],
            )
        else:
            # connect to default aws s3
            s3 = boto3.session.Session().client(
                "s3",
                aws_access_key_id=dest["key"],
                aws_secret_access_key=dest["secret"],
            )

    s3.put_object(Body=body, Bucket=dest["bucket"], Key=path)


# no-op on write to help with debugging/profiling
def write_to_null(body, path, dest):
    pass


# write to local file system (mostly to support tests)
def write_to_file(body, path, dest):
    # prepend the bucket name to the path
    bucket_path = os.path.join(dest["bucket"], path)
    dirname = os.path.dirname(bucket_path)
    # create the directory structre if it doesn't exist
    if not os.path.exists(dirname):
        pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)
    # write the file
    if (type(body) is bytes):
        with open(bucket_path, "wb+") as outfile:
            outfile.write(body)
    else:
        with open(bucket_path, "w+") as outfile:
            outfile.write(str(body))

# save proto tile file
def save_tile(tile, dest, model_id, run_id, time_res, writer):
    tile = json.loads(tile)
    if tile["content"] is None:
        return None
    z, x, y = from_str_coord(tile["coord"])
    content = tile['content']

    path = f"{model_id}/{run_id}/{time_res}/{tile['feature']}/tiles/{tile['timestamp']}-{z}-{x}-{y}.tile"
    writer(base64.b64decode(content), path, dest)
    return tile

# Saves the tile as string representation of the proto buff. This is used for testing and debugging tile files. 
def save_tile_to_str(tile, dest, model_id, run_id, time_res, writer):
    tile = json.loads(tile)
    if tile["content"] is None:
        return None
    tile_pb = tiles_pb2.Tile()
    tile_pb.ParseFromString(base64.b64decode(tile['content']))

    z = tile_pb.coord.z
    x = tile_pb.coord.x
    y = tile_pb.coord.y

    path = f"{model_id}/{run_id}/{time_res}/{tile['feature']}/tiles/{tile['timestamp']}-{z}-{x}-{y}.txt"
    writer(str(tile_pb), path, dest)

# write timeseries to json in S3
def timeseries_to_json(df, dest, model_id, run_id, feature, time_res, column, writer):
    col_map = {}
    col_map[column] = "value"

    # Save the result to s3
    body = df.rename(columns=col_map, inplace=False).to_json(orient="records")
    path = f"{model_id}/{run_id}/{time_res}/{feature}/timeseries/{column}.json"
    writer(body, path, dest)


# save timeseries as csv
def save_timeseries_as_csv(
    df,
    qualifier_col,
    dest,
    model_id,
    run_id,
    time_res,
    timeseries_agg_columns,
    qualifier_map,
    writer,
):
    feature = df["feature"].values[0]

    if len(qualifier_col) == 0:
        timeseries_to_csv(
            df, dest, model_id, run_id, feature, time_res, timeseries_agg_columns, writer
        )
        return len(df.index)
    elif feature in qualifier_map and qualifier_col[0] in qualifier_map[feature]:
        for agg_col in timeseries_agg_columns:
            qualifier = qualifier_col[0]
            qualifier_values = (
                df[["timestamp", agg_col] + qualifier_col]
                .groupby("timestamp")
                .agg(list)
                .apply(
                    lambda x: dict(zip(["timestamp"] + x[qualifier], [x.name] + x[agg_col])), axis=1
                )
            )
            qualifier_df = pd.DataFrame(qualifier_values.tolist())
            qualifier_timeseries_to_csv(
                qualifier_df, dest, model_id, run_id, feature, time_res, agg_col, qualifier, writer
            )

    return -1


# write timeseries to json in S3
def timeseries_to_csv(
    df, dest, model_id, run_id, feature, time_res, timeseries_agg_columns, writer
):
    path = f"{model_id}/{run_id}/{time_res}/{feature}/timeseries/global/global.csv"
    body = df[["timestamp"] + timeseries_agg_columns].to_csv(index=False)
    writer(body, path, dest)


# write timeseries to json in S3
def qualifier_timeseries_to_csv(
    df, dest, model_id, run_id, feature, time_res, agg_column, qualifier, writer
):
    path = f"{model_id}/{run_id}/{time_res}/{feature}/timeseries/qualifiers/{qualifier}/{agg_column}.csv"

    # Save the result to s3
    body = df.to_csv(index=False)
    writer(body, path, dest)


# write raw data to json file in S3
def raw_data_to_csv(df, dest, model_id, run_id, time_res, threshold, output_columns, writer):
    num_rows = len(df.index)
    if num_rows <= threshold:
        feature = df["feature"].values[0]
        body = df[output_columns].to_csv(index=False)
        path = f"{model_id}/{run_id}/{time_res}/{feature}/raw/raw.csv"
        writer(body, path, dest)
    return num_rows


# save output values to json array
def output_values_to_json_array(df):
    pdf = df.rename(columns={"feature": "name"}).compute()
    json_str = pdf.to_json(orient="records")
    return json.loads(json_str)


# save any generic info as a json file (gadm regions lists, qualifier lists, etc)
def info_to_json(contents, dest, model_id, run_id, feature, filename, writer):
    path = f"{model_id}/{run_id}/raw/{feature}/info/{filename}.json"
    body = str(json.dumps(contents))
    writer(body, path, dest)


# save the results file
def results_to_json(contents, dest, model_id, run_id, writer):
    path = f"{model_id}/{run_id}/results/results.json"
    body = str(json.dumps(contents))
    writer(body, path, dest)

def to_proto(df):
    tile_coord = df["tile"].iloc[0]
    z, x, y = from_str_coord(tile_coord)
    if z < 0 or x < 0 or y < 0:
        return None

    # Protobuf tile object
    tile = tiles_pb2.Tile()
    tile.coord.z = z
    tile.coord.x = x
    tile.coord.y = y

    tile.bins.totalBins = int(
        math.pow(4, from_str_coord(df["subtile"].iloc[0])[0] - z)
    )  # Total number of bins (subtile) for the tile

    subtiles = df['subtile'].tolist()
    s_sum_t_sum = df['s_sum_t_sum'].tolist()
    s_sum_t_mean = df['s_sum_t_mean'].tolist()
    s_count = df['s_count'].tolist()

    for i in range(len(subtiles)):
        bin_index = project(subtiles[i], tile_coord)
        tile.bins.stats[bin_index].s_sum_t_sum += s_sum_t_sum[i]
        tile.bins.stats[bin_index].s_sum_t_mean += s_sum_t_mean[i]
        tile.bins.stats[bin_index].weight += s_count[i]

    tile_content = tile.SerializeToString()
    # To convert bytes to b64 encoded string value since json string doesn't support bytes
    b64encoded_content = base64.b64encode(tile_content).decode('utf-8')

    return json.dumps({
        "feature": f"{df['feature'].iloc[0]}",
        "timestamp": f"{df['timestamp'].iloc[0]}", 
        "coord": to_tile_coord(z, x, y),
        "content":  b64encoded_content,
    })

# convert given datetime object to monthly epoch timestamp
def to_normalized_time(date, time_res):
    def time_in_seconds(date, time_res):
        if date is pd.NaT:
            return 0

        if time_res == "month":
            return int(
                datetime.datetime(
                    date.year, date.month, 1, tzinfo=datetime.timezone.utc
                ).timestamp()
            )
        elif time_res == "year":
            return int(datetime.datetime(date.year, 1, 1, tzinfo=datetime.timezone.utc).timestamp())
        elif time_res == "all":
            return 0  # just put everything under one timestamp
        else:
            raise ValueError("time_res must be 'month' or 'year'")

    return time_in_seconds(date, time_res) * 1000


# Get storage option
def get_storage_options(target):
    options = {
        "anon": False,
        "use_ssl": False,
        "key": target["key"],
        "secret": target["secret"],
        "client_kwargs": {
            "region_name": target["region_name"],
            "endpoint_url": target["endpoint_url"],
        },
    }
    return options


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


def save_regional_stats(
    df,
    dest,
    model_id,
    run_id,
    time_res,
    agg_columns,
    region_level,
    writer,
):
    # Note: we want to limit the number of rows that will be saved with minimum/maximum value
    # For example, in a worst case scenario, let's say the value of all the rows in a dataset is 0,
    # we don't want to save all the rows where each value is 0 which is also the min/max value.
    max_num_items = 20

    feature = df["feature"].values[0]

    result = {"min": {}, "max": {}}

    # Calculate min and max for all aggregated value columns
    max_values = df[agg_columns].max()
    min_values = df[agg_columns].min()

    for col in agg_columns:
        select_cols = ["region_id", "timestamp", col]
        rows_with_min_df = df[df[col] == min_values[col]][select_cols].rename(
            columns={col: "value"}
        )
        result["min"][col] = rows_with_min_df.nlargest(max_num_items, ["timestamp"]).to_dict(
            orient="records"
        )

        rows_with_max_df = df[df[col] == max_values[col]][select_cols].rename(
            columns={col: "value"}
        )
        result["max"][col] = rows_with_max_df.nlargest(max_num_items, ["timestamp"]).to_dict(
            orient="records"
        )

    path = f"{model_id}/{run_id}/{time_res}/{feature}/regional/{region_level}/stats/default/extrema.json"
    body = str(json.dumps(result))
    writer(body, path, dest)


# Save regional aggregation data to csv
def save_regional_aggregation(
    df,
    dest,
    model_id,
    run_id,
    time_res,
    agg_columns,
    region_level,
    qualifier_map,
    qualifier_col,
    writer,
):
    feature = df["feature"].values[0]
    timestamp = df["timestamp"].values[0]

    if len(qualifier_col) == 0:
        df = df[["region_id"] + agg_columns]
        df = df.rename(columns={"region_id": "id"})

        path = f"{model_id}/{run_id}/{time_res}/{feature}/regional/{region_level}/aggs/{timestamp}/default/default.csv"
        body = df.to_csv(index=False)
        writer(body, path, dest)
    elif feature in qualifier_map and qualifier_col[0] in qualifier_map[feature]:
        qualifier = qualifier_col[0]
        df = df[["region_id"] + qualifier_col + agg_columns]
        df = df.rename(columns={"region_id": "id", qualifier: "qualifier"})

        path = f"{model_id}/{run_id}/{time_res}/{feature}/regional/{region_level}/aggs/{timestamp}/qualifiers/{qualifier}.csv"
        body = df.to_csv(index=False, mode="a")
        writer(body, path, dest)


# Save regional timeseries data to csv
def save_regional_timeseries(
    df,
    dest,
    model_id,
    run_id,
    time_res,
    timeseries_agg_columns,
    region_level,
    qualifier_map,
    qualifier_col,
    writer,
):
    feature = df["feature"].values[0]
    region_id = df["region_id"].values[0]

    if len(qualifier_col) == 0:
        df = df[["timestamp"] + timeseries_agg_columns]

        path = f"{model_id}/{run_id}/{time_res}/{feature}/regional/{region_level}/timeseries/default/{region_id}.csv"
        body = df.to_csv(index=False)
        writer(body, path, dest)
    elif feature in qualifier_map and qualifier_col[0] in qualifier_map[feature]:
        qualifier = qualifier_col[0]
        qualifier_value = df[qualifier_col[0]].values[0]
        df = df[["timestamp"] + timeseries_agg_columns]

        path = f"{model_id}/{run_id}/{time_res}/{feature}/regional/{region_level}/timeseries/qualifiers/{qualifier}/{qualifier_value}/{region_id}.csv"
        body = df.to_csv(index=False)
        writer(body, path, dest)


# Compute timeseries by region
def compute_timeseries_by_region(
    temporal_df,
    dest,
    model_id,
    run_id,
    time_res,
    region_level,
    qualifier_map,
    qualifier_cols,
    weight_column,
    writer,
):
    admin_level = REGION_LEVELS.index(region_level)
    regions_cols = extract_region_columns(temporal_df)

    temporal_df = temporal_df.assign(
        region_id=join_region_columns(temporal_df, regions_cols, admin_level)
    )
    # persist the result in memory since this df is going to be used for multiple qualifiers
    temporal_df = temporal_df.persist()

    # Iterate through all the qualifier columns. Not all columns map to
    # all the features, they will be excluded when processing each feature
    for qualifier_col in qualifier_cols:
        # TODO: Optimization: remove spatial 'mean' aggregation since spatial mean can be calculated on the fly in `wm-go` by `spatial sum / spatial count`
        # In order to achieve this, we first need to implement on the fly `spatial sum / spatial count` calculation in `wm-go`
        (timeseries_df, timeseries_agg_columns) = run_spatial_aggregation(
            temporal_df,
            ["feature", "region_id", "timestamp"] + qualifier_col,
            ["sum", "mean"],
            weight_column,
        )
        timeseries_df = (
            timeseries_df.repartition(npartitions=12)
            .groupby(["feature", "region_id"] + qualifier_col)
            .apply(
                lambda x: save_regional_timeseries(
                    x,
                    dest,
                    model_id,
                    run_id,
                    time_res,
                    timeseries_agg_columns,
                    region_level,
                    qualifier_map,
                    qualifier_col,
                    writer,
                ),
                meta=(None, "string[pyarrow]"),
            )
        )
        timeseries_df.compute()


# Save subtile stats to csv
def save_subtile_stats(df, dest, model_id, run_id, time_res, writer):
    df = df.sort_values(by=["zoom"])
    feature = df["feature"].values[0]
    timestamp = df["timestamp"].values[0]
    columns = df.columns.tolist()
    columns.remove("feature")
    columns.remove("timestamp")

    path = f"{model_id}/{run_id}/{time_res}/{feature}/stats/grid/{timestamp}.csv"
    body = df[columns].to_csv(index=False)
    writer(body, path, dest)


# Compute min/max stats of subtiles (grid cells) at each zoom level
def compute_subtile_stats(
    subtile_df, dest, model_id, run_id, time_res, min_precision, max_precision, writer
):
    print(
        f"\ncompute subtile stats dataframe length={len(subtile_df.index)}, npartitions={subtile_df.npartitions}\n"
    )

    result_dfs = []

    for level_idx in range(max_precision + 1):
        actual_level = max_precision - level_idx
        if actual_level < min_precision:
            continue

        print(f"Compute subtile stats at level {actual_level}")

        tile_at_actual_level = subtile_df.apply(
            lambda x: parent_tile(x.subtile, level_idx),
            axis=1,
            meta=(None, "string[pyarrow]"),
        )
        df = subtile_df.assign(subtile=tile_at_actual_level)

        # Group data points by unique subtile and sum their values and counts up
        df = df.groupby(["feature", "timestamp", "subtile"]).agg("sum")
        df = df.reset_index()

        # Compute mean from sum and count
        # Note that s_mean_t_wavg is calculated in subtile_aggregation
        df = df.assign(
            s_mean_t_sum=df["s_sum_t_sum"] / df["s_count"],
            s_mean_t_mean=df["s_sum_t_mean"] / df["s_count"],
        )

        # Extract zoom level from subtile coordinates
        zoom = df["subtile"].apply(lambda x: x[0], meta=("subtile", "int8"))
        df = df.assign(zoom=zoom).drop(["subtile", "s_count"], axis=1)
        # Group by zoom level and compute min and max value
        df = df.groupby(["feature", "timestamp", "zoom"]).agg(["min", "max"])
        # Flatten multi index columns to single index e.g (s_sum_t_sum, min) -> min_s_sum_t_sum
        df.columns = ["_".join(tuple(reversed(cols))) for cols in df.columns.to_flat_index()]
        df = df.reset_index()

        df = df.compute()
        result_dfs.append(df)

    result_df = dd.from_pandas(
        pd.concat(result_dfs, ignore_index=True), npartitions=DEFAULT_PARTITIONS
    )

    # Save the stats for each timestamp
    result_df = result_df.groupby(["feature", "timestamp"]).apply(
        lambda x: save_subtile_stats(x, dest, model_id, run_id, time_res, writer),
        meta=(None, "string[pyarrow]"),
    )
    result_df.compute()
