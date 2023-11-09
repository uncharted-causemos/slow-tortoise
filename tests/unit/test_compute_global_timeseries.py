
import boto3
from moto import mock_s3

import pandas as pd
import dask.dataframe as dd
from prefect.engine.signals import SKIP

from ..utils import execute_prefect_task, S3_DEST
from flows.data_pipeline import compute_global_timeseries, DEFAULT_PARTITIONS


def test_compute_global_timeseries_skip():
    df = dd.from_pandas(pd.DataFrame({}), npartitions=DEFAULT_PARTITIONS)

    result = execute_prefect_task(compute_global_timeseries)(df, {}, "month", "mid1", "rid1", {}, [], "", True)
    assert type(result) == SKIP
    assert str(result) == "'compute_global_timeseries' is skipped."

@mock_s3
def test_compute_global_timeseries():
    # setup mock s3 storage
    conn = boto3.resource("s3")
    conn.create_bucket(Bucket=S3_DEST["bucket"])

    columns = ["timestamp","country","feature","t_sum", "t_mean"]
    data = [
        # t1
        [0,"Ethiopia","feature1",4.0, 2.0], 
        [0,"South Sudan","feature1",60.0, 30.0],

        # t2
        [1,"Ethiopia","feature1",6.0, 3.0], 
        [1,"South Sudan","feature1",80, 0.8], 

        # feature 2
        [1,"Ethiopia","feature2",6.0, 3.0], 
        [1,"South Sudan","feature2",180, 1.8], 
    ]

    df = dd.from_pandas(pd.DataFrame(data, columns=columns), npartitions=DEFAULT_PARTITIONS)
    result = execute_prefect_task(compute_global_timeseries)(df, S3_DEST, "month", "mid1", "rid1", {}, [], "")

    print(conn)
    print(result)

    assert False

@mock_s3
def test_compute_global_timeseries_with_qualifiers():
    # # setup mock s3 storage
    # conn = boto3.resource("s3")
    # conn.create_bucket(Bucket=S3_DEST["bucket"])

    # columns = ["timestamp","country","feature","t_sum", "t_mean"]
    # data = [
    #     # t1
    #     [0,"Ethiopia","feature1",4.0, 2.0], 
    #     [0,"South Sudan","feature1",60.0, 30.0],

    #     # t2
    #     [1,"Ethiopia","feature1",6.0, 3.0], 
    #     [1,"South Sudan","feature1",80, 0.8], 

    #     # feature 2
    #     [1,"Ethiopia","feature2",6.0, 3.0], 
    #     [1,"South Sudan","feature2",180, 1.8], 
    # ]

    # df = dd.from_pandas(pd.DataFrame(data, columns=columns), npartitions=DEFAULT_PARTITIONS)
    # result = execute_prefect_task(compute_global_timeseries)(df, S3_DEST, "month", "mid1", "rid1", {}, [], "")

    # print(conn)
    # print(result)
    assert True