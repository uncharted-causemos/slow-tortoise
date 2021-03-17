from dask.distributed import Client
import dask.dataframe as dd
import dask.bytes as db
import datetime
import pandas as pd
import math
import boto3

client = Client('10.65.18.58:8786')
client.upload_file('tiles_pb2.py')
client
print(client)

import tiles_pb2

from prefect import task, Flow, Parameter
from prefect.engine.signals import SKIP


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



# More details on tile calculations https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames

# Convert lat, long to tile coord
# https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames#Python
def deg2num(lat_deg, lon_deg, zoom):
  lat_rad = math.radians(lat_deg)
  n = 2.0 ** zoom
  xtile = int((lon_deg + 180.0) / 360.0 * n)
  ytile = int((1.0 - math.asinh(math.tan(lat_rad)) / math.pi) / 2.0 * n)
  return (zoom, xtile, ytile)

# Get the parent tile coord of the given tile coord
def parent_tile(coord):
    z, x, y = coord
    return (z - 1, math.floor(x / 2), math.floor(y / 2))

# Return all acestor tile coords of the given tile coord
def ancestor_tiles(coord, min_zoom=0):
    tiles = [coord]
    while tiles[0][0] > min_zoom:
        tiles.insert(0, parent_tile(tiles[0]))
    return tiles

# Filter tiles by minimum zoom level
def filter_by_min_zoom(tiles, min_zoom=0):
    return list(filter(lambda x: x[0] >= min_zoom, tiles))
    
    
# Return the tile that is leveldiff up of given tile. Eg. return (1, 0, 0) for (6, 0, 0) with leveldiff = 5
# The main tile will contain up to 4^leveldiff subtiles with same level
def tile_coord(coord, leveldiff=LEVEL_DIFF):
    z, x, y = coord
    return (z - leveldiff, math.floor(x / math.pow(2, leveldiff)), math.floor(y / math.pow(2, leveldiff)))

# project subtile coord into xy coord of the main tile grid (n*n grid where n*n = 4^zdiff)
# https://wiki.openstreetmap.org/wiki/Slippy_map_tilenames
def project(subtilecoord, tilecoord):
    z, x, y = tilecoord
    sz, sx, sy = subtilecoord
    zdiff = sz - z # zoom level (prececsion) difference
    
    # Calculate the x and y of the coordinate of the subtile located at the most top left corner of the main tile
    offset_x = math.pow(2, zdiff) * x
    offset_y = math.pow(2, zdiff) * y
    
    # Project subtile coordinate to n * n (n * n = 4^zdiff) grid coordinate
    binx = sx - offset_x
    biny = sy - offset_y
    
    # Total number of grid cells
    total_bins = math.pow(4, zdiff)
    max_x_bins = math.sqrt(total_bins)
    
    bin_index = binx + biny*max_x_bins
    
    return int(bin_index)

# save proto tile file
def save_tile(tile, dest, model_id, run_id, feature, time_res, timestamp):
    # Create s3 client only if it hasn't been created in current worker
    # since initalizing the client is expensive. Make sure we only initialize it once per worker
    global s3
    if 's3' not in globals():
        s3 = boto3.session.Session().client(
            's3',
            endpoint_url=dest['endpoint_url'],
            region_name=dest['region_name'],
            aws_access_key_id=dest['key'],
            aws_secret_access_key=dest['secret']
        )
        
    z = tile.coord.z
    x = tile.coord.x
    y = tile.coord.y
    ## HACK: currently our existing system expects unix timestamps in milliseconds
    #t = timestamp * 1000
    t = 0 # temporarily for lpjml data for testing
    path = f'{model_id}/{run_id}/{time_res}/{feature}/tiles/{t}-{z}-{x}-{y}.tile'
    s3.put_object(Body=tile.SerializeToString(), Bucket=dest['bucket'], Key=path)
    return tile

# save timeseries as a json file
def save_timeseries(x, dest, model_id, run_id, feature, time_res, column):
    bucket = dest['bucket']
    x.to_json(f's3://{bucket}/{model_id}/{run_id}/{time_res}/{feature}/timeseries/{column}.json', orient='records',
        storage_options={
        'anon': False,
        'use_ssl': False,
        'key': dest['key'],
        'secret': dest['secret'],
        'client_kwargs':{
            'region_name': dest['region_name'],
            'endpoint_url': dest['endpoint_url']
        }
    })
    
# save stats as a json file
def save_stats(x, dest, model_id, run_id, feature, time_res):
    bucket = dest['bucket']
    x.to_json(f's3://{bucket}/{model_id}/{run_id}/{time_res}/{feature}/stats/stats.json', orient='index',
        storage_options={
        'anon': False,
        'use_ssl': False,
        'key': dest['key'],
        'secret': dest['secret'],
        'client_kwargs':{
            'region_name': dest['region_name'],
            'endpoint_url': dest['endpoint_url']
        }
    })
    
# transform given row to tile protobuf
def to_proto(row):
    z, x, y = row.tile
    
    tile = tiles_pb2.Tile()
    tile.coord.z = z
    tile.coord.x = x
    tile.coord.y = y
    
    tile.bins.totalBins = int(math.pow(4, row.subtile[0][0] - z)) # Total number of bins (subtile) for the tile
    
    for i in range(len(row.subtile)):
        bin_index = project(row.subtile[i], row.tile)
        tile.bins.stats[bin_index].sum += row.t_mean_s_sum[i]
        tile.bins.stats[bin_index].count += row.t_mean_s_count[i]
    # Calculate the average
    for bin_stat in tile.bins.stats.values():
        bin_stat.avg = bin_stat.sum / bin_stat.count
    return tile

# convert given datetime object to monthly epoch timestamp
def to_normalized_time(date, time_res):
    if time_res == 'month':
        return int(datetime.datetime(date.year, date.month, 1).timestamp())
    elif time_res == 'year':
        return int(datetime.datetime(date.year, 1, 1).timestamp())
    else:
        raise ValueError('time_res must be \'month\' or \'year\'')

#############################################################################

@task
def download_data(source, model_id, run_id):
    # Read parquet files in as set of dataframes
    bucket = source['bucket']
    df = dd.read_parquet(f's3://{bucket}/{model_id}/{run_id}/*.parquet',
        storage_options={
            'anon': False,
            'use_ssl': False,
            'key': source['key'],
            'secret': source['secret'],
            'client_kwargs':{
                'region_name': source['region_name'],
                'endpoint_url': source['endpoint_url']
            }
        }).repartition(npartitions = 100)
    # Ensure types
    df = df.astype({'value': 'float64'})
    df.dtypes
    return df

@task(skip_on_upstream_skip=False)
def temporal_aggregation(df, time_res):
    # Monthly temporal aggregation (compute for both sum and mean)
    df['timestamp'] = dd.to_datetime(df['timestamp']).apply(lambda x: to_normalized_time(x, time_res), meta=(None, 'int'))
    df = df.groupby(['feature', 'timestamp', 'lat', 'lng'])['value'].agg(['sum', 'mean'])

    # Rename agg column names
    df.columns = df.columns.str.replace('sum', 't_sum').str.replace('mean', 't_mean')
    df = df.reset_index()
    df.compute()
    return df

@task
def compute_timeseries(df, dest, time_res, model_id, run_id):
    # Timeseries aggregation
    timeseries_aggs = ['min', 'max', 'sum', 'mean']
    timeseries_lookup = {
        ('t_sum', 'min'): 'min_sum', ('t_sum', 'max'): 'max_sum', ('t_sum', 'sum'): 'sum_bin_sum', ('t_sum', 'mean'): 'avg_bin_sum',
        ('t_mean', 'min'): 'min_avg', ('t_mean', 'max'): 'max_avg', ('t_mean', 'sum'): 'sum_bin_avg', ('t_mean', 'mean'): 'avg_bin_avg'
    }
    timeseries_agg_columns = ['min_sum', 'max_sum', 'sum_bin_sum', 'avg_bin_sum', 'min_avg', 'max_avg', 'sum_bin_avg', 'avg_bin_avg']

    timeseries_df = df.groupby(['feature', 'timestamp']).agg({ 't_sum' : timeseries_aggs, 't_mean' : timeseries_aggs })
    timeseries_df.columns = timeseries_df.columns.to_flat_index()
    timeseries_df = timeseries_df.rename(columns=timeseries_lookup).reset_index()
    timeseries_df = timeseries_df.groupby(['feature']).apply(
        lambda x: [ save_timeseries(x[['timestamp', col]], dest, model_id, run_id, x['feature'].values[0], time_res, col) for col in timeseries_agg_columns],
        meta=(None, 'object'))
    timeseries_df.compute()

@task
def spatial_aggregation(df):
    # Spatial aggregation to the higest supported precision(subtile z) level
    df['subtile'] = df.apply(lambda x: deg2num(x.lat, x.lng, MAX_SUBTILE_PRECISION), axis=1, meta=(None, 'object'))
    df = df[['feature', 'timestamp', 'subtile', 't_sum', 't_mean']] \
        .groupby(['feature', 'timestamp', 'subtile']) \
        .agg(['sum', 'mean', 'count'])

    # Rename columns
    spatial_lookup = {('t_sum', 'sum'): 't_sum_s_sum', ('t_sum', 'mean'): 't_sum_s_mean', ('t_sum', 'count'): 't_sum_s_count',
            ('t_mean', 'sum'): 't_mean_s_sum', ('t_mean', 'mean'): 't_mean_s_mean', ('t_mean', 'count'): 't_mean_s_count'}
    df.columns = df.columns.to_flat_index()
    df = df.rename(columns=spatial_lookup).reset_index()
    df.compute()
    return df

@task
def compute_stats(df, dest, time_res, model_id, run_id):
    #Stats aggregation
    stats_aggs = ['min', 'max']
    stats_lookup = {
        ('t_sum_s_sum', 'min'): 'min_t_sum_s_sum', ('t_sum_s_sum', 'max'): 'max_t_sum_s_sum',
        ('t_sum_s_mean', 'min'): 'min_t_sum_s_mean', ('t_sum_s_mean', 'max'): 'max_t_sum_s_mean',
        ('t_mean_s_sum', 'min'): 'min_t_mean_s_sum', ('t_mean_s_sum', 'max'): 'max_t_mean_s_sum',
        ('t_mean_s_mean', 'min'): 'min_t_mean_s_mean', ('t_mean_s_mean', 'max'): 'max_t_mean_s_mean'
    }
    stats_agg_columns = ['min_t_sum_s_sum', 'max_t_sum_s_sum', 'min_t_sum_s_mean', 'max_t_sum_s_mean',
                        'min_t_mean_s_sum', 'max_t_mean_s_sum', 'min_t_mean_s_mean', 'max_t_mean_s_mean']

    stats_df = df.groupby(['feature']).agg({ 't_sum_s_sum' : stats_aggs, 't_sum_s_mean' : stats_aggs, 't_mean_s_sum' : stats_aggs, 't_mean_s_mean' : stats_aggs })
    stats_df.columns = stats_df.columns.to_flat_index()
    stats_df = stats_df.rename(columns=stats_lookup).reset_index()
    stats_df = stats_df.groupby(['feature']).apply(
        lambda x: save_stats(x[stats_agg_columns], dest, model_id, run_id, x['feature'].values[0], time_res),
        meta=(None, 'object'))
    stats_df.compute()

@task
def compute_tiling(df, should_run, dest, time_res, model_id, run_id):
    if should_run is False:
        raise SKIP("Tiling was not requested")
    
    # Get all acestor subtiles and explode
    # TODO: Instead of exploding, try reducing down by processing from higest zoom levels to lowest zoom levels one by one level. 
    df['subtile'] = df.apply(lambda x: filter_by_min_zoom(ancestor_tiles(x.subtile), MIN_SUBTILE_PRECISION), axis=1, meta=(None, 'object'))
    df = df.explode('subtile').repartition(npartitions = 100)

    # Assign main tile coord for each subtile
    df['tile'] = df.apply(lambda x: tile_coord(x.subtile, LEVEL_DIFF), axis=1, meta=(None, 'object'))

    df = df.groupby(['feature', 'timestamp', 'tile']) \
        .agg(list) \
        .reset_index() \
        .repartition(npartitions = 200) \
        .apply(lambda x: save_tile(to_proto(x), dest, model_id, run_id, x.feature, time_res, x.timestamp), axis=1, meta=(None, 'object'))  # convert each row to protobuf and save
    df.compute()


###########################################################################

with Flow('datacube-ingest-v0.1') as flow:

    # Parameters
    model_id = Parameter('model_id', default='e0a14dbf-e8e6-42bd-b908-e72a956fadd5')
    run_id = Parameter('run_id', default='749916f0-be24-4e4b-9a6c-798808a5be3c')
    compute_tiles = Parameter('compute_tiles', default=False)

    source = Parameter('source', default = {
        'endpoint_url': 'http://10.65.18.73:9000',
        'region_name':'us-east-1',
        'key': 'foobar',
        'secret': 'foobarbaz',
        'bucket': 'airflow-test-data'
    })

    dest = Parameter('dest', default = {
        'endpoint_url': 'http://10.65.18.73:9000',
        'region_name': 'us-east-1',
        'key': 'foobar',
        'secret': 'foobarbaz',
        'bucket': 'mass-upload-test'
    })

    df = download_data(source, model_id, run_id)

    # ==== Run aggregations based on monthly time resolution =====
    monthly_data = temporal_aggregation(df, 'month')
    month_ts_done = compute_timeseries(monthly_data, dest, 'month', model_id, run_id)

    monthly_spatial_data = spatial_aggregation(monthly_data, upstream_tasks=[month_ts_done])
    month_stats_done = compute_stats(monthly_spatial_data, dest, 'month', model_id, run_id)
    month_done = compute_tiling(monthly_spatial_data, compute_tiles, dest, 'month', model_id, run_id, upstream_tasks=[month_stats_done])

    # ==== Run aggregations based on annual time resolution =====
    annual_data = temporal_aggregation(df, 'year', upstream_tasks=[month_done])
    year_ts_done = compute_timeseries(annual_data, dest, 'year', model_id, run_id)

    annual_spatial_data = spatial_aggregation(annual_data, upstream_tasks=[year_ts_done])
    year_stats_done = compute_stats(annual_spatial_data, dest, 'year', model_id, run_id)
    compute_tiling(annual_spatial_data, compute_tiles, dest, 'year', model_id, run_id, upstream_tasks=[year_stats_done])

    ## TODO: Saving intermediate result as a file (for each feature) and storing in our minio might be useful. 
    ## Then same data can be used for producing tiles and also used for doing regional aggregation and other computation in other tasks.
    ## In that way we can have one jupyter notbook or python module for each tasks


flow.register(project_name='Tiling')

# from prefect.executors import DaskExecutor
# executor = DaskExecutor(address="tcp://10.65.18.58:8786")
# state = flow.run(executor=executor)
