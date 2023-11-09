import boto3
import json
from moto import mock_s3

import pandas as pd
import dask.dataframe as dd
from prefect.engine.signals import SKIP

from ..utils import (
    execute_prefect_task,
    assert_csv_frame_equal,
    assert_json_equal,
    read_obj,
    S3_DEST,
)
from flows.data_pipeline import compute_global_timeseries, DEFAULT_PARTITIONS


def test_compute_global_timeseries_skip():
    df = dd.from_pandas(pd.DataFrame({}), npartitions=DEFAULT_PARTITIONS)

    result = execute_prefect_task(compute_global_timeseries)(
        df, {}, "month", "mid1", "rid1", {}, [], "", True
    )
    assert type(result) == SKIP
    assert str(result) == "'compute_global_timeseries' is skipped."


@mock_s3
def test_compute_global_timeseries():
    # connect to mock s3 storage
    s3 = boto3.resource("s3")
    bucket = s3.create_bucket(Bucket=S3_DEST["bucket"])

    columns = ["timestamp", "country", "feature", "t_sum", "t_mean"]
    data = [
        # t1
        [0, "Ethiopia", "feature1", 4.0, 2.0],
        [0, "South Sudan", "feature1", 60.0, 30.0],
        # t2
        [1, "Ethiopia", "feature1", 6.0, 3.0],
        [1, "South Sudan", "feature1", 80, 0.8],
        # feature 2
        [1, "Ethiopia", "feature2", 6.0, 3.0],
        [1, "South Sudan", "feature2", 180, 1.8],
    ]

    df = dd.from_pandas(pd.DataFrame(data, columns=columns), npartitions=DEFAULT_PARTITIONS)
    result = execute_prefect_task(compute_global_timeseries)(
        df, S3_DEST, "month", "model-id-1", "run-id-1", {}, [], ""
    )

    # assertions

    assert {"feature1": 2, "feature2": 1} == json.loads(result)

    f1_output = read_obj(s3, "model-id-1/run-id-1/month/feature1/timeseries/global/global.csv")
    assert_csv_frame_equal(
        """timestamp,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
                           0,64.0,32.0,32.0,16.0,2
                           1,86.0,43.0,3.8,1.9,2
                           """,
        f1_output,
    )
    f2_output = read_obj(s3, "model-id-1/run-id-1/month/feature2/timeseries/global/global.csv")
    assert_csv_frame_equal(
        """timestamp,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
                           1,186.0,93.0,4.8,2.4,2
                           """,
        f2_output,
    )


@mock_s3
def test_compute_global_timeseries_with_qualifiers():
    # connect to mock s3 storage
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket=S3_DEST["bucket"])

    columns = ["timestamp", "country", "feature", "qual1", "qual2", "t_sum", "t_mean"]
    data = [
        # t1
        [0, "Ethiopia", "feature1", "qa", "q1", 4.0, 2.0],
        [0, "South Sudan", "feature1", "qa", "q2", 60.0, 30.0],
        [0, "South Sudan", "feature1", "qb", "q2", 60.0, 30.0],
        # t2
        [1, "Ethiopia", "feature1", "qa", "q1", 6.0, 3.0],
        [1, "South Sudan", "feature1", "qb", "q1", 80, 0.8],
        # feature 2
        [1, "Ethiopia", "feature2", "qa", "q1", 6.0, 3.0],
        [1, "South Sudan", "feature2", "qa", "q2", 180, 1.8],
        # feature 3
        [1, "Ethiopia", "feature3", "qa", "q1", 6.0, 3.0],
    ]

    df = dd.from_pandas(pd.DataFrame(data, columns=columns), npartitions=DEFAULT_PARTITIONS)
    qual_map = {"feature1": ["qual1", "qual2"], "feature2": ["qual1"]}
    qual_cols = [["qual1"], ["qual2"]]
    result = execute_prefect_task(compute_global_timeseries)(
        df, S3_DEST, "year", "model-id-2", "run-id-2", qual_map, qual_cols, ""
    )

    # assertions
    assert {"feature1": 2, "feature2": 1, "feature3": 1} == json.loads(result)

    assert_csv_frame_equal(
        """timestamp,qa,qb
                           0,2,1
                           1,1,1
                           """,
        read_obj(s3, "model-id-2/run-id-2/year/feature1/timeseries/qualifiers/qual1/s_count.csv"),
    )
    assert_csv_frame_equal(
        """timestamp,qa,qb
                           0,16.0,30.0
                           1,3.0,0.8
                           """,
        read_obj(
            s3, "model-id-2/run-id-2/year/feature1/timeseries/qualifiers/qual1/s_mean_t_mean.csv"
        ),
    )
    assert_csv_frame_equal(
        """timestamp,qa,qb
                           0,32.0,30.0
                           1,3.0,0.8
                           """,
        read_obj(
            s3, "model-id-2/run-id-2/year/feature1/timeseries/qualifiers/qual1/s_sum_t_mean.csv"
        ),
    )

    assert_csv_frame_equal(
        """timestamp,q1,q2
                           0,4.0,60.0
                           1,43.0,
                           """,
        read_obj(
            s3, "model-id-2/run-id-2/year/feature1/timeseries/qualifiers/qual2/s_mean_t_sum.csv"
        ),
    )

    assert_csv_frame_equal(
        """timestamp,qa
                           1,186.0
                           """,
        read_obj(
            s3, "model-id-2/run-id-2/year/feature2/timeseries/qualifiers/qual1/s_sum_t_sum.csv"
        ),
    )
