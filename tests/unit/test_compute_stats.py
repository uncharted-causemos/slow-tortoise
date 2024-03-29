import boto3
from moto import mock_s3

import pandas as pd
import dask.dataframe as dd

from ..utils import (
    read_obj,
    assert_csv_frame_equal,
    execute_prefect_task,
    S3_DEST,
)

from flows.data_pipeline import compute_stats, DEFAULT_PARTITIONS


@mock_s3
def test_compute_stats():
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

    execute_prefect_task(compute_stats)(df, S3_DEST, "month", "model-id-1", "run-id-1")

    assert_csv_frame_equal(
        """zoom,min_s_sum_t_sum,max_s_sum_t_sum,min_s_sum_t_mean,max_s_sum_t_mean,min_s_mean_t_sum,max_s_mean_t_sum,min_s_mean_t_mean,max_s_mean_t_mean
        6,96.0,120.0,48.0,60.0,32.0,60.0,16.0,30.0
        7,96.0,120.0,48.0,60.0,32.0,60.0,16.0,30.0
        8,96.0,120.0,48.0,60.0,32.0,60.0,16.0,30.0
        9,96.0,120.0,48.0,60.0,32.0,60.0,16.0,30.0
        10,96.0,120.0,48.0,60.0,32.0,60.0,16.0,30.0
        11,96.0,120.0,48.0,60.0,32.0,60.0,16.0,30.0
        12,96.0,120.0,48.0,60.0,32.0,60.0,16.0,30.0
        13,96.0,120.0,48.0,60.0,32.0,60.0,16.0,30.0
        14,96.0,120.0,48.0,60.0,32.0,60.0,16.0,30.0
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F1/stats/grid/0.csv"),
        sort_by=["zoom"],
    )
    assert_csv_frame_equal(
        """zoom,min_s_sum_t_sum,max_s_sum_t_sum,min_s_sum_t_mean,max_s_sum_t_mean,min_s_mean_t_sum,max_s_mean_t_sum,min_s_mean_t_mean,max_s_mean_t_mean
        6,80.0,90.0,1.8,40.0,26.666666666666668,45.0,0.9,13.333333333333334
        7,80.0,90.0,1.8,40.0,26.666666666666668,45.0,0.9,13.333333333333334
        8,80.0,90.0,1.8,40.0,26.666666666666668,45.0,0.9,13.333333333333334
        9,80.0,90.0,1.8,40.0,26.666666666666668,45.0,0.9,13.333333333333334
        10,80.0,90.0,1.8,40.0,26.666666666666668,45.0,0.9,13.333333333333334
        11,80.0,90.0,1.8,40.0,26.666666666666668,45.0,0.9,13.333333333333334
        12,80.0,90.0,1.8,40.0,26.666666666666668,45.0,0.9,13.333333333333334
        13,80.0,90.0,1.8,40.0,26.666666666666668,45.0,0.9,13.333333333333334
        14,80.0,90.0,1.8,40.0,26.666666666666668,45.0,0.9,13.333333333333334
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F1/stats/grid/1.csv"),
        sort_by=["zoom"],
    )
    assert_csv_frame_equal(
        """zoom,min_s_sum_t_sum,max_s_sum_t_sum,min_s_sum_t_mean,max_s_sum_t_mean,min_s_mean_t_sum,max_s_mean_t_sum,min_s_mean_t_mean,max_s_mean_t_mean
        6,96.0,120.0,48.0,60.0,32.0,60.0,16.0,30.0
        7,96.0,120.0,48.0,60.0,32.0,60.0,16.0,30.0
        8,96.0,120.0,48.0,60.0,32.0,60.0,16.0,30.0
        9,96.0,120.0,48.0,60.0,32.0,60.0,16.0,30.0
        10,96.0,120.0,48.0,60.0,32.0,60.0,16.0,30.0
        11,96.0,120.0,48.0,60.0,32.0,60.0,16.0,30.0
        12,96.0,120.0,48.0,60.0,32.0,60.0,16.0,30.0
        13,96.0,120.0,48.0,60.0,32.0,60.0,16.0,30.0
        14,96.0,120.0,48.0,60.0,32.0,60.0,16.0,30.0
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F2/stats/grid/0.csv"),
        sort_by=["zoom"],
    )
    assert_csv_frame_equal(
        """zoom,min_s_sum_t_sum,max_s_sum_t_sum,min_s_sum_t_mean,max_s_sum_t_mean,min_s_mean_t_sum,max_s_mean_t_sum,min_s_mean_t_mean,max_s_mean_t_mean
        6,80.0,90.0,1.8,40.0,26.666666666666668,45.0,0.9,13.333333333333334
        7,80.0,90.0,1.8,40.0,26.666666666666668,45.0,0.9,13.333333333333334
        8,80.0,90.0,1.8,40.0,26.666666666666668,45.0,0.9,13.333333333333334
        9,80.0,90.0,1.8,40.0,26.666666666666668,45.0,0.9,13.333333333333334
        10,80.0,90.0,1.8,40.0,26.666666666666668,45.0,0.9,13.333333333333334
        11,80.0,90.0,1.8,40.0,26.666666666666668,45.0,0.9,13.333333333333334
        12,80.0,90.0,1.8,40.0,26.666666666666668,45.0,0.9,13.333333333333334
        13,80.0,90.0,1.8,40.0,26.666666666666668,45.0,0.9,13.333333333333334
        14,80.0,90.0,1.8,40.0,26.666666666666668,45.0,0.9,13.333333333333334
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F2/stats/grid/1.csv"),
        sort_by=["zoom"],
    )
