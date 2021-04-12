import datetime
import pandas as pd
import dask.dataframe as dd
import math
import boto3

import tiles_pb2

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

# save timeseries as a json file
def save_timeseries(df, dest, model_id, run_id, time_res, timeseries_agg_columns):
    for col in timeseries_agg_columns:
        timeseries_to_json(df[['timestamp', col]], dest, model_id, run_id, df['feature'].values[0], time_res, col)

# write timeseries to json
def timeseries_to_json(x, dest, model_id, run_id, feature, time_res, column):
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
def stats_to_json(x, dest, model_id, run_id, feature, time_res):
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
        tile.bins.stats[bin_index].count += row.s_count[i]
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

# Get storage option
def get_storage_option(target):
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

def join_region_columns(df, level=3, deli='|'):
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
            region_agg[region_id] = {'t_sum_s_sum': 0, 't_mean_s_sum': 0, 's_count': 0}
        
        region_agg[region_id]['t_sum_s_sum'] += x['t_sum_s_sum'][i]
        region_agg[region_id]['t_mean_s_sum'] += x['t_mean_s_sum'][i]
        region_agg[region_id]['s_count'] += x['s_count'][i]

    # Compute mean    
    for key in region_agg:
        region_agg[key]['t_sum_s_mean'] = region_agg[key]['t_sum_s_sum'] / region_agg[key]['s_count']
        region_agg[key]['t_mean_s_mean'] = region_agg[key]['t_mean_s_sum'] / region_agg[key]['s_count']

    # to Json
    result = {'t_mean_s_sum': [], 't_mean_s_mean': [], 't_sum_s_sum': [], 't_sum_s_mean': [] }
    for key in region_agg:
        result['t_mean_s_sum'].append({ 'id': key, 'value': region_agg[key]['t_mean_s_sum']})
        result['t_mean_s_mean'].append({ 'id': key, 'value': region_agg[key]['t_mean_s_mean']})
        result['t_sum_s_sum'].append({ 'id': key, 'value': region_agg[key]['t_sum_s_sum']})
        result['t_sum_s_mean'].append({ 'id': key, 'value': region_agg[key]['t_sum_s_mean']})
    # Save the result to s3
    save_regional_aggregation_to_s3(result, dest, model_id, run_id, time_res, region_level, feature, timestamp)
    return result

def save_regional_aggregation_to_s3(agg_result, dest, model_id, run_id, time_res, region_level, feature, timestamp):
    bucket = dest['bucket']
    for key in agg_result:
        save_df = pd.DataFrame(agg_result[key])
        save_df.to_json(f's3://{bucket}/{model_id}/{run_id}/{time_res}/{feature}/regional/{region_level}/aggs/{timestamp}/{key}.json',
                        orient='records',
                        storage_options=get_storage_option(dest))

def extract_region_columns(df):
    region_col_names = ['country', 'admin1', 'admin2', 'admin3']
    columns = df.columns.to_list()
    # find the intersection
    result = list(set(region_col_names) & set(columns)) 
    # Re order the list by admin levels
    result.sort()
    if 'country' in result:
        result.remove('country')
        result.insert(0, 'country')
    return result