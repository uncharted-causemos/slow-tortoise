from dask import delayed
from typing import Tuple
import dask.dataframe as dd
import dask.bytes as db
import pandas as pd
import requests
import boto3
import os

from prefect import task, Flow, Parameter
from prefect.engine.signals import SKIP
from prefect.storage import Docker
from prefect.executors import DaskExecutor, LocalDaskExecutor
from flows.common import deg2num, ancestor_tiles, filter_by_min_zoom, \
    tile_coord, save_tile, save_timeseries, \
    stats_to_json, feature_to_json, to_proto, to_normalized_time, \
    extract_region_columns, join_region_columns, save_regional_aggregation, \
    output_values_to_json_array, raw_data_to_json, compute_timeseries_by_region, \
    write_to_file, write_to_null, write_to_s3


# Maps a write type to writing function
WRITE_TYPES = {
    "s3": write_to_s3, # write to an s3 bucket
    "file": write_to_file, # write to the local file system
    "none": write_to_null # skip write for debugging
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
S3_DEFAULT_INDICATOR_BUCKET = os.getenv("WM_S3_DEFAULT_INDICATOR_BUCKET", 'indicators')

# default model s3 write bucket
S3_DEFAULT_MODEL_BUCKET = os.getenv("WM_S3_DEFAULT_MODEL_BUCKET", 'models')

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

MIN_SUBTILE_PRECISION = LEVEL_DIFF # since (0,0,0) main tile wil have (LEVEL_DIFF, x, y) subtiles as its grid cells

# Maximum zoom level for a main tile
MAX_ZOOM = MAX_SUBTILE_PRECISION - LEVEL_DIFF

ELASTIC_MODEL_RUN_INDEX = 'data-model-run'
ELASTIC_INDICATOR_INDEX = 'data-datacube'

@task(log_stdout=True)
def download_data(source, data_paths, is_indicator):
    df = None
    # if source is from s3 bucket
    if 's3://' in data_paths[0]:
        df = dd.read_parquet(data_paths,
            storage_options={
                'anon': False,
                'use_ssl': False,
                'key': source['key'],
                'secret': source['secret'],
                'client_kwargs':{
                    'region_name': source['region_name'],
                    'endpoint_url': source['endpoint_url']
                }
            }).repartition(npartitions = 12)
    else:
        # In some parquet files the value column will be type string. Filter out those parquet files and ignore for now
        numeric_files = []
        string_files = []
        for path in data_paths:
            if path.endswith('_str.parquet.gzip'):
                string_files.append(path)
            else:
                numeric_files.append(path)

        # Note: dask read_parquet doesn't work for gzip files. So here is the work around using pandas read_parquet
        dfs = [delayed(pd.read_parquet)(path) for path in numeric_files]
        df = dd.from_delayed(dfs).repartition(npartitions = 12)

    # These columns are empty for indicators and they seem to break the pipeline
    if is_indicator:
        df = df.drop(columns=['lat', 'lng'])

    # Ensure types
    df = df.astype({'value': 'float64'})
    df.dtypes
    return df

@task(log_stdout=True)
def configure_pipeline(dest, indicator_bucket, model_bucket, compute_tiles, is_indicator) -> Tuple[dict, str, bool, bool, bool, bool, bool]:
    if is_indicator:
        dest['bucket'] = indicator_bucket
        elastic_index = ELASTIC_INDICATOR_INDEX
        compute_raw = True
        compute_monthly = True
        compute_annual = True
        compute_summary = False
        compute_tiles = False
    else:
        dest['bucket'] = model_bucket
        elastic_index = ELASTIC_MODEL_RUN_INDEX
        compute_raw = False
        compute_monthly = True
        compute_annual = True
        compute_summary = True

    return (dest, elastic_index, compute_raw, compute_monthly, compute_annual, compute_summary, compute_tiles)

@task(log_stdout=True)
def save_raw_data(df, dest, time_res, model_id, run_id, should_run):
    if should_run is False:
        raise SKIP('Saving raw data was not requested')

    raw_df = df.copy()
    output_columns = ['timestamp', 'country', 'admin1', 'admin2', 'admin3', 'value']

    raw_df = raw_df.groupby(['feature']).apply(
        lambda x: raw_data_to_json(x[output_columns], dest, model_id, run_id, time_res, x['feature'].values[0], WRITE_TYPES[DEST_TYPE]),
        meta=(None, 'object'))
    raw_df.compute()

@task(log_stdout=True)
def remove_null_region_columns(df):
    region_cols = extract_region_columns(df)
    cols_to_drop = set(df.columns[df.isnull().all()])
    region_cols_to_drop = list(cols_to_drop.intersection(region_cols))
    return df.drop(columns=region_cols_to_drop)

@task(skip_on_upstream_skip=False)
def temporal_aggregation(df, time_res, should_run):
    if should_run is False:
        raise SKIP(f'Aggregating for resolution {time_res} was not requested')

    columns = df.columns.tolist()
    columns.remove('value')

    # Monthly temporal aggregation (compute for both sum and mean)
    t = dd.to_datetime(df['timestamp'], unit='ms').apply(lambda x: to_normalized_time(x, time_res), meta=(None, 'int'))
    temporal_df = df.assign(timestamp=t) \
                    .groupby(columns)['value'].agg(['sum', 'mean'])

    # Rename agg column names
    temporal_df.columns = temporal_df.columns.str.replace('sum', 't_sum').str.replace('mean', 't_mean')
    temporal_df = temporal_df.reset_index()
    return temporal_df

@task(log_stdout=True)
def compute_timeseries(df, dest, time_res, model_id, run_id):
    # Timeseries aggregation
    timeseries_aggs = ['min', 'max', 'sum', 'mean']
    timeseries_lookup = {
        ('t_sum', 'min'): 's_min_t_sum', ('t_sum', 'max'): 's_max_t_sum', ('t_sum', 'sum'): 's_sum_t_sum', ('t_sum', 'mean'): 's_mean_t_sum',
        ('t_mean', 'min'): 's_min_t_mean', ('t_mean', 'max'): 's_max_t_mean', ('t_mean', 'sum'): 's_sum_t_mean', ('t_mean', 'mean'): 's_mean_t_mean'
    }
    timeseries_agg_columns = ['s_min_t_sum', 's_max_t_sum', 's_sum_t_sum', 's_mean_t_sum', 's_min_t_mean', 's_max_t_mean', 's_sum_t_mean', 's_mean_t_mean']

    timeseries_df = df.groupby(['feature', 'timestamp']).agg({ 't_sum' : timeseries_aggs, 't_mean' : timeseries_aggs })
    timeseries_df.columns = timeseries_df.columns.to_flat_index()
    timeseries_df = timeseries_df.rename(columns=timeseries_lookup).reset_index()
    timeseries_df = timeseries_df.groupby(['feature']).apply(
        lambda x: save_timeseries(x, dest, model_id, run_id, time_res, timeseries_agg_columns, WRITE_TYPES[DEST_TYPE]),
        meta=(None, 'object'))
    timeseries_df.compute()

@task(log_stdout=True)
def subtile_aggregation(df, should_run):
    if should_run is False:
        raise SKIP('Tiling was not requested')

    # Spatial aggregation to the higest supported precision(subtile z) level

    stile = df.apply(lambda x: deg2num(x.lat, x.lng, MAX_SUBTILE_PRECISION), axis=1, meta=(None, 'object'))
    subtile_df = df.assign(subtile=stile)
    subtile_df = subtile_df[['feature', 'timestamp', 'subtile', 't_sum', 't_mean']] \
        .groupby(['feature', 'timestamp', 'subtile']) \
        .agg(['sum', 'count'])

    # Rename columns
    spatial_lookup = {('t_sum', 'sum'): 's_sum_t_sum', ('t_sum', 'count'): 's_count_t_sum',
            ('t_mean', 'sum'): 's_sum_t_mean', ('t_mean', 'count'): 's_count'}
    subtile_df.columns = subtile_df.columns.to_flat_index()
    subtile_df = subtile_df.rename(columns=spatial_lookup).drop(columns='s_count_t_sum').reset_index()
    return subtile_df

@task(log_stdout=True)
def compute_tiling(df, dest, time_res, model_id, run_id):
    # Get all acestor subtiles and explode
    # TODO: Instead of exploding, try reducing down by processing from higest zoom levels to lowest zoom levels one by one level.
    stile = df.apply(lambda x: filter_by_min_zoom(ancestor_tiles(x.subtile), MIN_SUBTILE_PRECISION), axis=1, meta=(None, 'object'))
    tiling_df = df.assign(subtile=stile)
    tiling_df = tiling_df.explode('subtile').repartition(npartitions = 100)

    # Assign main tile coord for each subtile
    tiling_df['tile'] = tiling_df.apply(lambda x: tile_coord(x.subtile, LEVEL_DIFF), axis=1, meta=(None, 'object'))

    tiling_df = tiling_df.groupby(['feature', 'timestamp', 'tile']) \
        .agg(list) \
        .reset_index() \
        .repartition(npartitions = 200) \
        .apply(lambda x: save_tile(to_proto(x), dest, model_id, run_id, x.feature, time_res, x.timestamp, WRITE_TYPES[DEST_TYPE]), axis=1, meta=(None, 'object'))  # convert each row to protobuf and save
    tiling_df.compute()

@task(log_stdout=True)
def compute_regional_aggregation(input_df, dest, time_res, model_id, run_id):
    # Copy input df so that original df doesn't get mutated
    df = input_df.copy()
    # Ranme columns
    df.columns = df.columns.str.replace('t_sum', 's_sum_t_sum').str.replace('t_mean', 's_sum_t_mean')
    df['s_count'] = 1
    df = df.reset_index()

    regions_cols = extract_region_columns(df)

    # Region aggregation at the highest admin level
    df = df[['feature', 'timestamp', 's_sum_t_sum', 's_sum_t_mean', 's_count'] + regions_cols] \
        .groupby(['feature', 'timestamp'] + regions_cols) \
        .agg(['sum'])
    df.columns = df.columns.droplevel(1)
    df = df.reset_index()
    # persist the result in memory at this point since this df is going to be used multiple times to compute for different regional levels
    df = df.persist()

    # Compute aggregation and save for all regional levels
    for level in range(len(regions_cols)):
        save_df = df.copy()
        # Merge region columns to single region_id column. eg. ['Ethiopia', 'Afar'] -> ['Ethiopia_Afar']
        save_df['region_id'] = join_region_columns(save_df, level)

        desired_columns = ['feature', 'timestamp', 'region_id', 's_sum_t_sum', 's_sum_t_mean', 's_count']
        save_df = save_df[desired_columns].groupby(['feature', 'timestamp']).agg(list)

        save_df = save_df.reset_index()
        # At this point data is already reduced to reasonably small size due to prior admin aggregation.
        # Just perform repartition to make sure save io operation runs in parallel since each writing operation is expensive and blocks
        # Set npartitions to same as # of available workers/threads. Increasing partition number beyond the number of the workers doesn't seem to give more performance benefits.
        save_df = save_df.repartition(npartitions = 12)
        save_df = save_df.apply(lambda x: save_regional_aggregation(x, dest, model_id, run_id, time_res, WRITE_TYPES[DEST_TYPE], region_level=regions_cols[level]),
                      axis=1, meta=(None, 'object'))
        save_df.compute()
    return df

@task
def compute_regional_aggregation_stats(regional_df, dest, timeframe, model_id, run_id):
    # Compute aggregation and save for all regional levels
    regions_cols = extract_region_columns(regional_df)
    for level in range(len(regions_cols)):
        df = regional_df.copy()
        # Merge region columns to single region_id column. eg. ['Ethiopia', 'Afar'] -> ['Ethiopia_Afar']
        df['region_id'] = join_region_columns(df, level)
        assist_compute_stats(df, dest, timeframe, model_id, run_id, f'regional/{regions_cols[level]}')

@task(log_stdout=True)
def compute_regional_timeseries(df, dest, model_id, run_id, time_res):
    regions_cols = extract_region_columns(df)
    for region_level in regions_cols:
        compute_timeseries_by_region(df, dest, model_id, run_id, time_res, region_level, WRITE_TYPES[DEST_TYPE])

@task(log_stdout=True)
def compute_stats(df, dest, time_res, model_id, run_id, filename):
    assist_compute_stats(df, dest, time_res, model_id, run_id, filename)

def assist_compute_stats(df, dest, time_res, model_id, run_id, filename):
    #Compute mean and get new dataframe with mean columns added
    stats_df = df.assign(s_mean_t_sum=df['s_sum_t_sum'] / df['s_count'], s_mean_t_mean=df['s_sum_t_mean'] / df['s_count'])
    #Stats aggregation
    stats_aggs = ['min', 'max']
    stats_lookup = {
        ('s_sum_t_sum', 'min'): 'min_s_sum_t_sum', ('s_sum_t_sum', 'max'): 'max_s_sum_t_sum',
        ('s_mean_t_sum', 'min'): 'min_s_mean_t_sum', ('s_mean_t_sum', 'max'): 'max_s_mean_t_sum',
        ('s_sum_t_mean', 'min'): 'min_s_sum_t_mean', ('s_sum_t_mean', 'max'): 'max_s_sum_t_mean',
        ('s_mean_t_mean', 'min'): 'min_s_mean_t_mean', ('s_mean_t_mean', 'max'): 'max_s_mean_t_mean'
    }
    stats_agg_columns = ['min_s_sum_t_sum', 'max_s_sum_t_sum', 'min_s_mean_t_sum', 'max_s_mean_t_sum',
                         'min_s_sum_t_mean', 'max_s_sum_t_mean', 'min_s_mean_t_mean', 'max_s_mean_t_mean']

    stats_df = stats_df.groupby(['feature']).agg({ 's_sum_t_sum' : stats_aggs, 's_mean_t_sum' : stats_aggs, 's_sum_t_mean' : stats_aggs, 's_mean_t_mean' : stats_aggs })
    stats_df.columns = stats_df.columns.to_flat_index()
    stats_df = stats_df.rename(columns=stats_lookup).reset_index()
    stats_df = stats_df.groupby(['feature']).apply(
        lambda x: stats_to_json(x[stats_agg_columns], dest, model_id, run_id, x['feature'].values[0], time_res, filename, WRITE_TYPES[DEST_TYPE]),
        meta=(None, 'object'))
    stats_df.compute()

@task(log_stdout=True)
def compute_output_summary(df):
    # Timeseries aggregation
    timeseries_aggs = ['mean']
    timeseries_agg_column = 's_mean_t_mean'
    timeseries_lookup = { ('t_mean', 'mean'): timeseries_agg_column }

    timeseries_df = df.groupby(['feature', 'timestamp']).agg({ 't_mean' : timeseries_aggs })
    timeseries_df.columns = timeseries_df.columns.to_flat_index()
    timeseries_df = timeseries_df.rename(columns=timeseries_lookup).reset_index()

    summary = output_values_to_json_array(timeseries_df[['feature', timeseries_agg_column]], timeseries_agg_column)
    return summary

@task(skip_on_upstream_skip=False, log_stdout=True)
def update_metadata(doc_ids, summary_values, elastic_url, elastic_index):
    data = {
        'doc' : {
            'status': 'READY'
        }
    }
    if summary_values is not None:
        data['doc']['output_agg_values'] = summary_values

    for doc_id in doc_ids:
        r = requests.post(f'{elastic_url}/{elastic_index}/_update/{doc_id}', json = data)
        print(r.text)
        r.raise_for_status()

@task(log_stdout=True)
def record_region_hierarchy(df, dest, model_id, run_id):
    region_cols = extract_region_columns(df)
    hierarchy = {}
    # This builds the hierarchy
    for index, row in df.iterrows():
        feature = row['feature']
        if feature not in hierarchy:
            hierarchy[feature] = {}
        current_hierarchy_position = hierarchy[feature]
        for region_id in range(len(region_cols) - 1):
            current_region = row[region_cols[region_id]]
            if current_region not in current_hierarchy_position:
                current_hierarchy_position[current_region] = {}
            current_hierarchy_position = current_hierarchy_position[current_region]
        current_hierarchy_position[row[region_cols[-1]]] = None
    feature_to_json(hierarchy, dest, model_id, run_id, 'hierarchy', WRITE_TYPES[DEST_TYPE])

###########################################################################

with Flow('datacube-ingest-v0.1') as flow:
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
        registry_url= registry_url,
        base_image="docker.uncharted.software/worldmodeler/wm-data-pipeline:latest",
        image_name=image_name,
        local_image=True,
        stored_as_script=True,
        path="/wm_data_pipeline/flows/tile-v0.py",
        ignore_healthchecks=True,
    )

    # Parameters
    model_id = Parameter('model_id', default='geo-test-data')
    run_id = Parameter('run_id', default='test-run')
    doc_ids = Parameter('doc_ids', default=[])
    data_paths = Parameter('data_paths', default=['s3://test/geo-test-data.parquet'])
    compute_tiles = Parameter('compute_tiles', default=False)
    is_indicator = Parameter('is_indicator', default=False)
    indicator_bucket = Parameter('indicator_bucket', default=S3_DEFAULT_INDICATOR_BUCKET)
    model_bucket = Parameter('model_bucket', default=S3_DEFAULT_MODEL_BUCKET)

    # skip write to elastic if URL unset - we define this and don't use it prefect
    # errors
    if ELASTIC_URL:
        elastic_url = Parameter('elastic_url', default=ELASTIC_URL)

    source = Parameter('source', default = {
        'endpoint_url': S3_SOURCE_URL,
        'region_name':'us-east-1',
        'key': 'foobar',
        'secret': 'foobarbaz'
    })


    dest = Parameter('dest', default = {
        'endpoint_url': S3_DEST_URL,
        'region_name': 'us-east-1',
        'key': 'foobar',
        'secret': 'foobarbaz'
    })

    raw_df = download_data(source, data_paths, is_indicator)

    # ==== Set parameters that determine which tasks should run based on the type of data we're ingesting ====
    (
        dest,
        elastic_index,
        compute_raw,
        compute_monthly,
        compute_annual,
        compute_summary,
        compute_tiles,
    ) = configure_pipeline(dest, indicator_bucket, model_bucket, compute_tiles, is_indicator)

    # ==== Save raw data =====
    save_raw_data(raw_df, dest, 'raw', model_id, 'indicator', compute_raw)

    df = remove_null_region_columns(raw_df)

    # ==== Compute high level features for current run =====
    record_region_hierarchy(df, dest, model_id, run_id)

    # ==== Run aggregations based on monthly time resolution =====
    monthly_data = temporal_aggregation(df, 'month', compute_monthly)
    month_ts_done = compute_timeseries(monthly_data, dest, 'month', model_id, run_id)
    compute_regional_timeseries(monthly_data, dest, model_id, run_id, 'month')
    monthly_regional_df = compute_regional_aggregation(monthly_data, dest, 'month', model_id, run_id)
    compute_regional_aggregation_stats(monthly_regional_df, dest, 'month', model_id, run_id)

    monthly_spatial_data = subtile_aggregation(monthly_data, compute_tiles, upstream_tasks=[month_ts_done])
    month_stats_done = compute_stats(monthly_spatial_data, dest, 'month', model_id, run_id, "stats")
    month_done = compute_tiling(monthly_spatial_data, dest, 'month', model_id, run_id, upstream_tasks=[month_stats_done])

    # ==== Run aggregations based on annual time resolution =====
    annual_data = temporal_aggregation(df, 'year', compute_annual, upstream_tasks=[month_done, month_ts_done])
    year_ts_done = compute_timeseries(annual_data, dest, 'year', model_id, run_id)
    compute_regional_timeseries(annual_data, dest, model_id, run_id, 'year')
    annual_regional_df = compute_regional_aggregation(annual_data, dest, 'year', model_id, run_id)
    compute_regional_aggregation_stats(annual_regional_df, dest, 'year', model_id, run_id)

    annual_spatial_data = subtile_aggregation(annual_data, compute_tiles, upstream_tasks=[year_ts_done])
    year_stats_done = compute_stats(annual_spatial_data, dest, 'year', model_id, run_id, "stats")
    year_done = compute_tiling(annual_spatial_data, dest, 'year', model_id, run_id, upstream_tasks=[year_stats_done])

    # ==== Generate a single aggregate value per feature =====
    summary_data = temporal_aggregation(df, 'all', compute_summary, upstream_tasks=[year_done, year_ts_done])
    summary_values = compute_output_summary(summary_data)

    # ==== Update document in ES setting the status to READY =====
    if ELASTIC_URL:
        update_metadata(doc_ids, summary_values, elastic_url, elastic_index)

    ## TODO: Saving intermediate result as a file (for each feature) and storing in our minio might be useful.
    ## Then same data can be used for producing tiles and also used for doing regional aggregation and other computation in other tasks.
    ## In that way we can have one jupyter notbook or python module for each tasks

# If this is a local run, just execute the flow in process.  Setting WM_DASK_SCHEDULER="" will result in a local cluster
# being run as well.
if __name__ == "__main__" and LOCAL_RUN:
    from prefect.utilities.debug import raise_on_exception
    with raise_on_exception():
        # flow.run(parameters=dict(is_indicator=True, model_id='ACLED', run_id='indicator', data_paths=['s3://test/acled/acled-test.bin']))
        # flow.run(parameters=dict(compute_tiles=True, model_id='geo-test-data', run_id='test-run', data_paths=['s3://test/geo-test-data.parquet']))
        flow.run(parameters=dict(
             compute_tiles=True,
             model_id='maxhop-v0.2',
             run_id='4675d89d-904c-466f-a588-354c047ecf72',
             data_paths=['https://jataware-world-modelers.s3.amazonaws.com/dmc_results/4675d89d-904c-466f-a588-354c047ecf72/4675d89d-904c-466f-a588-354c047ecf72_maxhop-v0.2.parquet.gzip']
        ))
