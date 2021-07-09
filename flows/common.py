import datetime
import pandas as pd
import math
import boto3
import json
import sys
import os

from flows import tiles_pb2

# Bit of a WTF here, but it is well considered.  Dask will serialize the tiles_pb2.Task *class* since it is passed
# to workers within a lambda that calls to_proto.  The problem is that pickling a class object can result in the
# parent *module* object being pickled depending on how its imported, and according to the pickling spec, module
# objects can't be pickled.  This manifests itself as an error on a Dask worker indicating that it can't serialize the
# tiles_pb2 module.  To get around this, we need to import tiles_pb2 module directly, instead of through the flow package,
# which means we need to add the parent directory to the sys path.
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))
import tiles_pb2

REGION_LEVELS = ['country', 'admin1', 'admin2', 'admin3']

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
def tile_coord(coord, leveldiff=6):
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

    path = f'{model_id}/{run_id}/{time_res}/{feature}/tiles/{timestamp}-{z}-{x}-{y}.tile'
    s3.put_object(Body=tile.SerializeToString(), Bucket=dest['bucket'], Key=path)
    return tile

# save timeseries as json
def save_timeseries(df, dest, model_id, run_id, time_res, timeseries_agg_columns):
    for col in timeseries_agg_columns:
        timeseries_to_json(df[['timestamp', col]], dest, model_id, run_id, df['feature'].values[0], time_res, col)

# write timeseries to json in S3
def timeseries_to_json(df, dest, model_id, run_id, feature, time_res, column):
    bucket = dest['bucket']
    col_map = {}
    col_map[column] = 'value'
    df.rename(columns=col_map, inplace=False).to_json(f's3://{bucket}/{model_id}/{run_id}/{time_res}/{feature}/timeseries/{column}.json',
        orient='records',
        storage_options=get_storage_options(dest))

# write raw data to json file in S3
def raw_data_to_json(df, dest, model_id, run_id, time_res, feature):
    bucket = dest['bucket']
    df.to_json(f's3://{bucket}/{model_id}/{run_id}/{time_res}/{feature}/raw/raw.json',
        orient='records',
        storage_options=get_storage_options(dest))

# save output values to json array
def output_values_to_json_array(df, column):
    col_map = { 'feature': 'name' }
    col_map[column] = 'value'
    pdf = df.rename(columns=col_map).compute()
    json_str = pdf.to_json(orient='records')
    return json.loads(json_str)

# save stats as a json file
def stats_to_json(x, dest, model_id, run_id, feature, time_res):
    bucket = dest['bucket']
    x.to_json(f's3://{bucket}/{model_id}/{run_id}/{time_res}/{feature}/stats/stats.json',
        orient='index',
        storage_options=get_storage_options(dest))

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
        tile.bins.stats[bin_index].s_sum_t_sum += row.s_sum_t_sum[i]
        tile.bins.stats[bin_index].s_sum_t_mean += row.s_sum_t_mean[i]
        tile.bins.stats[bin_index].weight += row.s_count[i]
    return tile

# convert given datetime object to monthly epoch timestamp
def to_normalized_time(date, time_res):
    def time_in_seconds():
        if time_res == 'month':
            return int(datetime.datetime(date.year, date.month, 1).timestamp())
        elif time_res == 'year':
            return int(datetime.datetime(date.year, 1, 1).timestamp())
        elif time_res == 'all':
            return 0 # just put everything under one timestamp
        else:
            raise ValueError('time_res must be \'month\' or \'year\'')
    return time_in_seconds() * 1000

# Get storage option
def get_storage_options(target):
    options = {
        'anon': False,
        'use_ssl': False,
        'key': target['key'],
        'secret': target['secret'],
        'client_kwargs':{
            'region_name': target['region_name'],
            'endpoint_url': target['endpoint_url']
        }
    }
    return options

def join_region_columns(df, level=3, deli='__'):
    if level == 3:
        return df['country'] + deli + df['admin1'] + deli + df['admin2'] + deli + df['admin3']
    elif level == 2:
        return df['country'] + deli + df['admin1'] + deli + df['admin2']
    elif level == 1:
        return df['country'] + deli + df['admin1']
    else:
        return df['country']

def save_regional_aggregation(x, dest, model_id, run_id, time_res, region_level='admin3'):
    feature = x.feature
    timestamp = x.timestamp

    region_agg = {}
    # Run sum up all values for each region.
    for i in range(len(x.region_id)):
        region_id = x.region_id[i]
        if region_id not in region_agg:
            region_agg[region_id] = {'s_sum_t_sum': 0, 's_sum_t_mean': 0, 's_count': 0}

        region_agg[region_id]['s_sum_t_sum'] += x['s_sum_t_sum'][i]
        region_agg[region_id]['s_sum_t_mean'] += x['s_sum_t_mean'][i]
        region_agg[region_id]['s_count'] += x['s_count'][i]

    # Compute mean
    for key in region_agg:
        region_agg[key]['s_mean_t_sum'] = region_agg[key]['s_sum_t_sum'] / region_agg[key]['s_count']
        region_agg[key]['s_mean_t_mean'] = region_agg[key]['s_sum_t_mean'] / region_agg[key]['s_count']

    # to Json
    result = {'s_sum_t_mean': [], 's_mean_t_mean': [], 's_sum_t_sum': [], 's_mean_t_sum': [] }
    for key in region_agg:
        result['s_sum_t_mean'].append({ 'id': key, 'value': region_agg[key]['s_sum_t_mean']})
        result['s_mean_t_mean'].append({ 'id': key, 'value': region_agg[key]['s_mean_t_mean']})
        result['s_sum_t_sum'].append({ 'id': key, 'value': region_agg[key]['s_sum_t_sum']})
        result['s_mean_t_sum'].append({ 'id': key, 'value': region_agg[key]['s_mean_t_sum']})
    # Save the result to s3
    save_regional_aggregation_to_s3(result, dest, model_id, run_id, time_res, region_level, feature, timestamp)
    return result

def save_regional_aggregation_to_s3(agg_result, dest, model_id, run_id, time_res, region_level, feature, timestamp):
    bucket = dest['bucket']
    for key in agg_result:
        save_df = pd.DataFrame(agg_result[key])
        save_df.to_json(f's3://{bucket}/{model_id}/{run_id}/{time_res}/{feature}/regional/{region_level}/aggs/{timestamp}/{key}.json',
                        orient='records',
                        storage_options=get_storage_options(dest))

def extract_region_columns(df):
    columns = df.columns.to_list()
    # find the intersection
    result = list(set(REGION_LEVELS) & set(columns))
    # Re order the list by admin levels
    result.sort()
    if 'country' in result:
        result.remove('country')
        result.insert(0, 'country')
    return result

# Save regional timeseries data to csv
def save_regional_timeseries(df, dest, model_id, run_id, time_res, timeseries_agg_columns, region_level):
    bucket = dest['bucket']
    feature = df['feature'].values[0]
    region_id = df['region_id'].values[0]
    df = df[['timestamp'] + timeseries_agg_columns]
    df.to_csv(f's3://{bucket}/{model_id}/{run_id}/{time_res}/{feature}/regional/{region_level}/timeseries/{region_id}.csv',
        storage_options=get_storage_options(dest), index=False)

# Compute timeseries by region
def compute_timeseries_by_region(temporal_df, dest, model_id, run_id, time_res, region_level):
    timeseries_df = temporal_df.copy()
    timeseries_df['region_id'] = join_region_columns(timeseries_df, REGION_LEVELS.index(region_level))
    timeseries_aggs = ['min', 'max', 'sum', 'mean', 'count']
    timeseries_lookup = {
        ('t_sum', 'min'): 's_min_t_sum', ('t_sum', 'max'): 's_max_t_sum', ('t_sum', 'sum'): 's_sum_t_sum', ('t_sum', 'mean'): 's_mean_t_sum',
        ('t_mean', 'min'): 's_min_t_mean', ('t_mean', 'max'): 's_max_t_mean', ('t_mean', 'sum'): 's_sum_t_mean', ('t_mean', 'mean'): 's_mean_t_mean', 
        ('t_mean', 'count'): 's_count_t_mean', ('t_sum', 'count'): 's_count'
    }
    timeseries_agg_columns = ['s_min_t_sum', 's_max_t_sum', 's_sum_t_sum', 's_mean_t_sum', 's_min_t_mean', 's_max_t_mean', 's_sum_t_mean', 's_mean_t_mean', 's_count']

    timeseries_df = timeseries_df.groupby(['feature', 'region_id', 'timestamp']).agg({ 't_sum' : timeseries_aggs, 't_mean' : timeseries_aggs })
    timeseries_df.columns = timeseries_df.columns.to_flat_index()
    timeseries_df = timeseries_df.rename(columns=timeseries_lookup).reset_index()
    timeseries_df = timeseries_df.repartition(npartitions = 12).groupby(['feature', 'region_id']).apply(
        lambda x: save_regional_timeseries(x, dest, model_id, run_id, time_res, timeseries_agg_columns, region_level), meta=(None, 'object'))
    timeseries_df.compute()
