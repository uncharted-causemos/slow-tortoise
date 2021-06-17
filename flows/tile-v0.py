from dask.distributed import Client
from dask import delayed
from typing import Tuple
import dask.dataframe as dd
import dask.bytes as db
import prefect
import datetime
import pandas as pd
import math
import boto3
import requests

from prefect import task, Flow, Parameter
from prefect.engine.signals import SKIP

from common import deg2num, parent_tile, ancestor_tiles, filter_by_min_zoom, \
    tile_coord, project, save_tile, save_timeseries, timeseries_to_json, \
    stats_to_json, to_proto, to_normalized_time, get_storage_options, \
    extract_region_columns, join_region_columns, save_regional_aggregation, \
    output_values_to_json_array, raw_data_to_json

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
        # Note: dask read_parquet doesn't work for gzip files. So here is the work around using pandas read_parquet
        dfs = [delayed(pd.read_parquet)(path) for path in data_paths]
        # dfs
        df = dd.from_delayed(dfs).repartition(npartitions = 12)
    
    # These columns are empty for indicators and they seem to break the pipeline
    if is_indicator:
        df = df.drop(columns=['lat', 'lng'])
    
    # Ensure types
    df = df.astype({'value': 'float64'})
    df.dtypes
    # print("download_data:")
    # print(df.compute())
    # coun = df['country'].compute()
    # print(coun)
    return df

@task
def configure_pipeline(dest, indicator_bucket, model_bucket, model_id, run_id, compute_tiles, is_indicator) -> Tuple[dict, str, str, bool, bool, bool, bool, bool]:
    if is_indicator:
        dest['bucket'] = indicator_bucket
        elastic_index = ELASTIC_INDICATOR_INDEX
        elastic_id = model_id
        compute_raw = True
        compute_monthly = True
        compute_annual = True
        compute_summary = False
        compute_tiles = False
    else:
        dest['bucket'] = model_bucket
        elastic_index = ELASTIC_MODEL_RUN_INDEX
        elastic_id = run_id
        compute_raw = False
        compute_monthly = True
        compute_annual = True
        compute_summary = True

    return (dest, elastic_index, elastic_id, compute_raw, compute_monthly, compute_annual, compute_summary, compute_tiles)

@task(skip_on_upstream_skip=False)
def temporal_aggregation(df, time_res, should_run):
    print("temporal_aggregation PRE: " + time_res)
    print(df.compute())
    if should_run is False:
        raise SKIP(f'Aggregating for resolution {time_res} was not requested')

    columns = df.columns.tolist()
    columns.remove('value')

    # is_ts_null = df['timestamp'].isnull().all().compute()
    # print("TIMESTAMP:")
    # print(df['timestamp'].compute())
    # if is_ts_null:
    #     df['timestamp'] = 0
    #     df.compute()
    #     print(df['timestamp'])
    # Monthly temporal aggregation (compute for both sum and mean)
    t = dd.to_datetime(df['timestamp'], unit='ms').apply(lambda x: to_normalized_time(x, time_res), meta=(None, 'int'))
    print("temporal_aggregation PRE-MIDDLE: " + time_res)
    # print(df.assign(timestamp=t).groupby(['timestamp', 'country', 'feature'])['value'].agg(['sum']).compute())
    temporal_df = df.assign(timestamp=t) \
                    .groupby(columns)['value'].agg(['sum', 'mean'])
    print("temporal_aggregation MIDDLE: " + time_res)
    print(temporal_df.compute())
    # Rename agg column names
    temporal_df.columns = temporal_df.columns.str.replace('sum', 't_sum').str.replace('mean', 't_mean')
    temporal_df = temporal_df.reset_index()
    print("temporal_aggregation POST: " + time_res)
    print(temporal_df.compute())
    print(temporal_df.shape[0].compute())
    print(temporal_df.shape[1])
    print("SHAPE^^^")
    return temporal_df 

@task
def compute_timeseries(df, dest, time_res, model_id, run_id):
    # print("compute_timeseries PRE: " + time_res)
    # df.compute()
    # print(df)
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
        lambda x: save_timeseries(x, dest, model_id, run_id, time_res, timeseries_agg_columns),
        meta=(None, 'object'))
    timeseries_df.compute()
    # print("compute_timeseries POST: " + time_res)
    # print(timeseries_df)

@task
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

@task
def compute_stats(df, dest, time_res, model_id, run_id):
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
        lambda x: stats_to_json(x[stats_agg_columns], dest, model_id, run_id, x['feature'].values[0], time_res),
        meta=(None, 'object'))
    stats_df.compute()

@task
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
        .apply(lambda x: save_tile(to_proto(x), dest, model_id, run_id, x.feature, time_res, x.timestamp), axis=1, meta=(None, 'object'))  # convert each row to protobuf and save
    tiling_df.compute()

@task(log_stdout=True)
def compute_regional_aggregation(input_df, dest, time_res, model_id, run_id, feature_name):
    # Copy input df so that original df doesn't get mutated
    df = input_df.copy()
    # Ranme columns
    df.columns = df.columns.str.replace('t_sum', 's_sum_t_sum').str.replace('t_mean', 's_sum_t_mean')
    df['s_count'] = 1
    df = df.reset_index()

    regions_cols = extract_region_columns(df)
    regions_cols_to_drop = list(df[regions_cols].columns[df[regions_cols].isnull().all()])
    # print("compute_regional_aggregation:")
    # coun = df['country'].compute()
    # print(coun)
    df = df.drop(columns=regions_cols_to_drop)
    # print(df)
    regions_cols = list(set(regions_cols).difference(set(regions_cols_to_drop)))
    
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

        cols_to_drop = list(save_df.columns[
            save_df.isnull().all()
        ])
        if 'feature' in cols_to_drop:
            cols_to_drop.remove('feature')
            # save_df['feature'] = feature_name
        if 'timestamp' in cols_to_drop:
            cols_to_drop.remove('timestamp')
        save_df = save_df.drop(columns=cols_to_drop)
        desired_columns = set(['feature', 'timestamp', 'region_id', 's_sum_t_sum', 's_sum_t_mean', 's_count'])
        desired_columns = list(set(save_df.columns).intersection(desired_columns))

        # try:
        save_df = save_df[desired_columns] \
            .groupby(['feature', 'timestamp']).agg(list)
        # except:
        #     save_df = save_df[desired_columns] \
        #         .groupby(['feature']).agg(list)
        save_df = save_df.reset_index()
        # At this point data is already reduced to reasonably small size due to prior admin aggregation. 
        # Just perform repartition to make sure save io operation runs in parallel since each writing operation is expensive and blocks
        # Set npartitions to same as # of available workers/threads. Increasing partition number beyond the number of the workers doesn't seem to give more performance benefits.
        save_df = save_df.repartition(npartitions = 12)
        save_df = save_df.apply(lambda x: save_regional_aggregation(x, dest, model_id, run_id, time_res, region_level=regions_cols[level]), 
                      axis=1, meta=(None, 'object'))
        save_df.compute()

@task
def save_raw_data(df, dest, time_res, model_id, run_id, should_run):
    if should_run is False:
        raise SKIP('Saving raw data was not requested')

    output_columns = ['timestamp', 'country', 'admin1', 'admin2', 'admin3', 'value']

    raw_df = df.groupby(['feature']).apply(
        lambda x: raw_data_to_json(x[output_columns], dest, model_id, run_id, time_res, x['feature'].values[0]),
        meta=(None, 'object'))
    raw_df.compute()

@task
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
def update_metadata(elastic_id, summary_values, elastic_url, elastic_index):
    data = {
        'doc' : {
            'status': 'READY'
        }
    }
    if summary_values is not None:
        data['doc']['output_agg_values'] = summary_values
    
    r = requests.post(f'{elastic_url}/{elastic_index}/_update/{elastic_id}', json = data)
    print(r.text)
    r.raise_for_status()

@task
def remove_null_region_columns(df):
    regions_cols = extract_region_columns(df)
    regions_cols_to_drop = list(df[regions_cols].columns[df[regions_cols].isnull().all()])
    return df.drop(columns=regions_cols_to_drop)

###########################################################################

with Flow('datacube-ingest-v0.1') as flow:
    # client = Client('10.65.18.58:8786')
    client = Client('192.168.0.105:8786')
    client.upload_file('/Users/vkorapaty/Desktop/wm/slow-tortoise/flows/tiles_pb2.py')
    client.upload_file('/Users/vkorapaty/Desktop/wm/slow-tortoise/flows/common.py')
    print(client)

    # Parameters
    model_id = Parameter('model_id', default='geo-test-data')
    run_id = Parameter('run_id', default='test-run')
    data_paths = Parameter('data_paths', default=['s3://test/geo-test-data.parquet'])
    compute_tiles = Parameter('compute_tiles', default=False)
    is_indicator = Parameter('is_indicator', default=False)
    elastic_url = Parameter('elastic_url', default='http://10.65.18.69:9200')
    indicator_bucket = Parameter('indicator_bucket', default='indicators')
    model_bucket = Parameter('model_bucket', default='models')
    feature_name = Parameter('feature_name', default='feature')

    source = Parameter('source', default = {
        'endpoint_url': 'http://10.65.18.73:9000',
        'region_name':'us-east-1',
        'key': 'foobar',
        'secret': 'foobarbaz'
    })

    dest = Parameter('dest', default = {
        'endpoint_url': 'http://10.65.18.73:9000',#'http://10.65.18.9:9000',
        'region_name': 'us-east-1',
        'key': 'foobar',
        'secret': 'foobarbaz'
    })

    df = download_data(source, data_paths, is_indicator)

    # ==== Set parameters that determine which tasks should run based on the type of data we're ingesting ====
    (
        dest,
        elastic_index,
        elastic_id,
        compute_raw,
        compute_monthly,
        compute_annual,
        compute_summary,
        compute_tiles,
    ) = configure_pipeline(dest, indicator_bucket, model_bucket, model_id, run_id, compute_tiles, is_indicator)

    # ==== Save raw data =====
    save_raw_data(df, dest, 'raw', model_id, 'indicator', compute_raw)

    df = remove_null_region_columns(df)
    # ==== Run aggregations based on monthly time resolution =====
    monthly_data = temporal_aggregation(df, 'month', compute_monthly)
    month_ts_done = compute_timeseries(monthly_data, dest, 'month', model_id, run_id)
    compute_regional_aggregation(monthly_data, dest, 'month', model_id, run_id, feature_name)

    monthly_spatial_data = subtile_aggregation(monthly_data, compute_tiles, upstream_tasks=[month_ts_done])
    month_stats_done = compute_stats(monthly_spatial_data, dest, 'month', model_id, run_id)
    month_done = compute_tiling(monthly_spatial_data, dest, 'month', model_id, run_id, upstream_tasks=[month_stats_done])

    # ==== Run aggregations based on annual time resolution =====
    annual_data = temporal_aggregation(df, 'year', compute_annual, upstream_tasks=[month_done])
    year_ts_done = compute_timeseries(annual_data, dest, 'year', model_id, run_id)
    compute_regional_aggregation(annual_data, dest, 'year', model_id, run_id, feature_name)

    annual_spatial_data = subtile_aggregation(annual_data, compute_tiles, upstream_tasks=[year_ts_done])
    year_stats_done = compute_stats(annual_spatial_data, dest, 'year', model_id, run_id)
    year_done = compute_tiling(annual_spatial_data, dest, 'year', model_id, run_id, upstream_tasks=[year_stats_done])

    # ==== Generate a single aggregate value per feature =====
    summary_data = temporal_aggregation(df, 'all', compute_summary, upstream_tasks=[year_done])
    summary_values = compute_output_summary(summary_data)

    # ==== Update document in ES setting the status to READY =====
    # update_metadata(elastic_id, summary_values, elastic_url, elastic_index)

    ## TODO: Saving intermediate result as a file (for each feature) and storing in our minio might be useful. 
    ## Then same data can be used for producing tiles and also used for doing regional aggregation and other computation in other tasks.
    ## In that way we can have one jupyter notbook or python module for each tasks

# flow.register(project_name='Tiling', labels=['dask'])

from prefect.executors import DaskExecutor
from prefect.utilities.debug import raise_on_exception
# import sys
# import json
# import os
# data_paths = json.loads(sys.argv[1])
# model_id = sys.argv[2]

def run(model_id, data_paths, feature_name):
    with raise_on_exception():
        executor = DaskExecutor(address='tcp://192.168.0.105:8786')
        # executor = DaskExecutor(address='tcp://10.65.18.58:8786') # Dask Dashboard: http://10.65.18.58:8787/status
        state = flow.run(executor=executor, parameters=dict(is_indicator=True, model_id=model_id, run_id='indicator', data_paths=data_paths, feature_name=feature_name))
    #     # state = flow.run(executor=executor, parameters=dict(compute_tiles=True, model_id='geo-test-data', run_id='test-run', data_paths=['s3://test/geo-test-data.parquet']))
    #     # state = flow.run(executor=executor, parameters=dict(compute_tiles=True, model_id='maxhop-v0.2', run_id='4675d89d-904c-466f-a588-354c047ecf72', data_paths=['https://jataware-world-modelers.s3.amazonaws.com/dmc_results/4675d89d-904c-466f-a588-354c047ecf72/4675d89d-904c-466f-a588-354c047ecf72_maxhop-v0.2.parquet.gzip']))