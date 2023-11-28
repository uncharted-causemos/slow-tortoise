import boto3
from moto import mock_s3


import pandas as pd
import dask.dataframe as dd

from ..utils import (
    execute_prefect_task,
    read_proto,
    assert_proto_equal,
    S3_DEST,
)

from flows import tiles_pb2
from flows.data_pipeline import compute_tiling, DEFAULT_PARTITIONS


# Helper to create a tile proto buf object
def create_tile(z: int, x: int, y: int, totalBins: int, stats: object):
    tile = tiles_pb2.Tile()
    tile.coord.x = x
    tile.coord.y = y
    tile.coord.z = z
    tile.bins.totalBins = totalBins

    for key, value in stats.items():
        tile.bins.stats[key].s_sum_t_sum = value["s_sum_t_sum"]
        tile.bins.stats[key].s_sum_t_mean = value["s_sum_t_mean"]
        tile.bins.stats[key].weight = value["weight"]
    return tile


@mock_s3
def test_compute_tiling():
    # connect to mock s3 storage
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket=S3_DEST["bucket"])

    columns = ["feature", "timestamp", "subtile", "s_sum_t_sum", "s_sum_t_mean", "s_count"]
    data = [
        ["F1", 0, (14, 10041, 7726), 96.0, 48.0, 3],
        ["F1", 0, (14, 9632, 7755), 120.0, 60.0, 2],
        ["F1", 1, (14, 10041, 7726), 80.0, 40.0, 3],
        ["F1", 1, (14, 9632, 7755), 90.0, 1.8, 2],
        ["F2", 0, (14, 10041, 7726), 96.0, 48.0, 3],
        ["F2", 0, (14, 9632, 7755), 120.0, 60.0, 2],
        ["F2", 1, (14, 10041, 7726), 80.0, 40.0, 3],
        ["F2", 1, (14, 9632, 7755), 90.0, 1.8, 2],
    ]
    df = dd.from_pandas(pd.DataFrame(data, columns=columns), npartitions=DEFAULT_PARTITIONS)

    execute_prefect_task(compute_tiling)(df, S3_DEST, "month", "model-id-1", "run-id-1")

    # zoom level 0
    assert_proto_equal(
        create_tile(
            0,
            0,
            0,
            4096,
            {
                1957: {"s_sum_t_sum": 120, "s_sum_t_mean": 60, "weight": 2},
                1959: {"s_sum_t_sum": 96, "s_sum_t_mean": 48, "weight": 3},
            },
        ),
        read_proto(s3, "model-id-1/run-id-1/month/F1/tiles/0-0-0-0.tile", tiles_pb2.Tile()),
    )
    assert_proto_equal(
        create_tile(
            0,
            0,
            0,
            4096,
            {
                1957: {"s_sum_t_sum": 90, "s_sum_t_mean": 1.8, "weight": 2},
                1959: {"s_sum_t_sum": 80, "s_sum_t_mean": 40, "weight": 3},
            },
        ),
        read_proto(s3, "model-id-1/run-id-1/month/F1/tiles/1-0-0-0.tile", tiles_pb2.Tile()),
    )

    # zoom level 1
    assert_proto_equal(
        create_tile(
            1,
            1,
            0,
            4096,
            {
                3851: {"s_sum_t_sum": 120, "s_sum_t_mean": 60, "weight": 2},
                3854: {"s_sum_t_sum": 96, "s_sum_t_mean": 48, "weight": 3},
            },
        ),
        read_proto(s3, "model-id-1/run-id-1/month/F1/tiles/0-1-1-0.tile", tiles_pb2.Tile()),
    )
    assert_proto_equal(
        create_tile(
            1,
            1,
            0,
            4096,
            {
                3851: {"s_sum_t_sum": 90, "s_sum_t_mean": 1.8, "weight": 2},
                3854: {"s_sum_t_sum": 80, "s_sum_t_mean": 40, "weight": 3},
            },
        ),
        read_proto(s3, "model-id-1/run-id-1/month/F2/tiles/1-1-1-0.tile", tiles_pb2.Tile()),
    )

    # level 8
    assert_proto_equal(
        create_tile(
            8,
            156,
            120,
            4096,
            {
                3001: {"s_sum_t_sum": 96, "s_sum_t_mean": 48, "weight": 3},
            },
        ),
        read_proto(s3, "model-id-1/run-id-1/month/F1/tiles/0-8-156-120.tile", tiles_pb2.Tile()),
    )
    assert_proto_equal(
        create_tile(
            8,
            156,
            120,
            4096,
            {
                3001: {"s_sum_t_sum": 80, "s_sum_t_mean": 40, "weight": 3},
            },
        ),
        read_proto(s3, "model-id-1/run-id-1/month/F1/tiles/1-8-156-120.tile", tiles_pb2.Tile()),
    )
    assert_proto_equal(
        create_tile(
            8,
            156,
            120,
            4096,
            {
                3001: {"s_sum_t_sum": 96, "s_sum_t_mean": 48, "weight": 3},
            },
        ),
        read_proto(s3, "model-id-1/run-id-1/month/F2/tiles/0-8-156-120.tile", tiles_pb2.Tile()),
    )
    assert_proto_equal(
        create_tile(
            8,
            150,
            121,
            4096,
            {
                736: {"s_sum_t_sum": 90, "s_sum_t_mean": 1.8, "weight": 2},
            },
        ),
        read_proto(s3, "model-id-1/run-id-1/month/F2/tiles/1-8-150-121.tile", tiles_pb2.Tile()),
    )
