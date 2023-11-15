import boto3
from moto import mock_s3

import pandas as pd
import dask.dataframe as dd
from prefect.engine.signals import SKIP

from ..utils import (
    execute_prefect_task,
    assert_csv_frame_equal,
    read_obj,
    S3_DEST,
)
from flows.data_pipeline import compute_regional_timeseries, DEFAULT_PARTITIONS


@mock_s3
def test_compute_global_timeseries_skip():
    df = dd.from_pandas(pd.DataFrame({}), npartitions=DEFAULT_PARTITIONS)

    qualifier_thresholds = {
        "max_count": 10000,
        "regional_timeseries_count": 100,
        "regional_timeseries_max_level": 1,
    }
    result = execute_prefect_task(compute_regional_timeseries)(
        df, {}, "month", "mid1", "rid1", {}, [], 0, {}, "", True
    )
    assert type(result) == SKIP
    assert str(result) == "'compute_regional_timeseries' is skipped."


@mock_s3
def test_compute_regional_timeseries():
    # connect to mock s3 storage
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket=S3_DEST["bucket"])

    columns = ["timestamp", "country", "admin1", "admin2", "admin3", "feature", "t_sum", "t_mean"]
    data = [
        # t1
        [0, "A", "AA", "AAA", "AAAA", "F1", 4.0, 2.0],
        [0, "A", "AA", "AAA", "AAAA", "F1", 10.0, 5.0],
        [0, "A", "AA", "AAB", "AABD", "F1", 8.0, 4.0],
        [0, "A", "AA", "AAB", "AABE", "F1", 3.0, 1.0],
        [0, "B", "BA", "BAB", "BABB", "F1", 60.0, 30.0],
        [0, "B", "BB", "BBB", "BBBB", "F1", 10.0, 2.0],
        # t2
        [1, "A", "AA", "AAA", "AAAA", "F1", 2.0, 1.0],
        [1, "A", "AA", "AAA", "AAAA", "F1", 4.0, 1.0],
        [1, "A", "AA", "AAB", "AABD", "F1", 8.0, 2.0],
        [1, "A", "AA", "AAB", "AABE", "F1", 10.0, 2.0],
        [1, "B", "BA", "BAB", "BABB", "F1", 90.0, 30.0],
        [1, "B", "BB", "BBB", "BBBB", "F1", 80.0, 20.0],
        # feature 2
        [1, "A", "AA", "AAA", "AAAA", "F2", 4.0, 2.0],
        [1, "A", "AA", "AAA", "AAAA", "F2", 10.0, 5.0],
    ]
    qualifier_thresholds = {
        "max_count": 10000,
        "regional_timeseries_count": 100,
        "regional_timeseries_max_level": 1,
    }

    df = dd.from_pandas(pd.DataFrame(data, columns=columns), npartitions=DEFAULT_PARTITIONS)
    result = execute_prefect_task(compute_regional_timeseries)(
        df, S3_DEST, "month", "model-id-1", "run-id-1", {}, [], 0, qualifier_thresholds, ""
    )

    # country level
    assert_csv_frame_equal(
        """timestamp,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        0,25.0,6.25,12.0,3.0,4
        1,24.0,6.0,6.0,1.5,4
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F1/regional/country/timeseries/default/A.csv"),
    )
    assert_csv_frame_equal(
        """timestamp,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        0,70.0,35.0,32.0,16.0,2
        1,170.0,85.0,50.0,25.0,2
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F1/regional/country/timeseries/default/B.csv"),
    )
    assert_csv_frame_equal(
        """timestamp,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        1,14.0,7.0,7.0,3.5,2
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F2/regional/country/timeseries/default/A.csv"),
    )

    # admin1 level
    assert_csv_frame_equal(
        """timestamp,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        0,25.0,6.25,12.0,3.0,4
        1,24.0,6.0,6.0,1.5,4
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F1/regional/admin1/timeseries/default/A__AA.csv"),
    )
    assert_csv_frame_equal(
        """timestamp,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        0,60.0,60.0,30.0,30.0,1
        1,90.0,90.0,30.0,30.0,1
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F1/regional/admin1/timeseries/default/B__BA.csv"),
    )
    assert_csv_frame_equal(
        """timestamp,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        0,10.0,10.0,2.0,2.0,1
        1,80.0,80.0,20.0,20.0,1
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F1/regional/admin1/timeseries/default/B__BB.csv"),
    )

    # admin3
    assert_csv_frame_equal(
        """timestamp,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        0,14.0,7.0,7.0,3.5,2
        1,6.0,3.0,2.0,1.0,2
        """,
        read_obj(
            s3,
            "model-id-1/run-id-1/month/F1/regional/admin3/timeseries/default/A__AA__AAA__AAAA.csv",
        ),
    )
    assert_csv_frame_equal(
        """timestamp,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
            0,3.0,3.0,1.0,1.0,1
            1,10.0,10.0,2.0,2.0,1
        """,
        read_obj(
            s3,
            "model-id-1/run-id-1/month/F1/regional/admin3/timeseries/default/A__AA__AAB__AABE.csv",
        ),
    )


@mock_s3
def test_compute_regional_timeseries_with_qualifiers():
    # connect to mock s3 storage
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket=S3_DEST["bucket"])

    columns = ["timestamp", "country", "admin1", "qual1", "qual2", "feature", "t_sum", "t_mean"]
    data = [
        # t1
        [0, "A", "AA", "qa", "q1", "F1", 4.0, 2.0],
        [0, "A", "AB", "qa", "q1", "F1", 10.0, 5.0],
        [0, "A", "AA", "qa", "q2", "F1", 4.0, 2.0],
        [0, "A", "AB", "qa", "q2", "F1", 10.0, 5.0],
        # t2
        [1, "A", "AA", "qa", "q1", "F1", 8.0, 2.0],
        [1, "A", "AB", "qa", "q1", "F1", 20.0, 10.0],
        [1, "A", "AA", "qa", "q2", "F1", 16.0, 4.0],
        [1, "A", "AB", "qa", "q2", "F1", 30.0, 15.0],
        [1, "A", "AB", "qa", "q2", "F2", 30.0, 15.0],
    ]
    qualifier_thresholds = {
        "max_count": 10000,
        "regional_timeseries_count": 100,
        "regional_timeseries_max_level": 1,
    }
    qual_cols = [["qual1"], ["qual2"]]
    qual_map = {"F1": ["qual1", "qual2"], "F2": ["qual1", "qual2"]}
    qual_counts = {"F1": {"qual1": 1, "qual2": 2}, "F2": {"qual1": 1, "qual2": 1}}
    df = dd.from_pandas(pd.DataFrame(data, columns=columns), npartitions=DEFAULT_PARTITIONS)

    execute_prefect_task(compute_regional_timeseries)(
        df,
        S3_DEST,
        "month",
        "model-id-1",
        "run-id-1",
        qual_map,
        qual_cols,
        qual_counts,
        qualifier_thresholds,
        "",
    )

    # should still produce regional aggregation without qualifier
    assert_csv_frame_equal(
        """timestamp,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        0,28.0,7.0,14.0,3.5,4
        1,74.0,18.5,31.0,7.75,4
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F1/regional/country/timeseries/default/A.csv"),
    )

    # output for country level by qualifier
    assert_csv_frame_equal(
        """timestamp,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        0,28.0,7.0,14.0,3.5,4
        1,74.0,18.5,31.0,7.75,4
        """,
        read_obj(
            s3, "model-id-1/run-id-1/month/F1/regional/country/timeseries/qualifiers/qual1/qa/A.csv"
        ),
    )
    assert_csv_frame_equal(
        """timestamp,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        0,14.0,7.0,7.0,3.5,2
        1,28.0,14.0,12.0,6.0,2
        """,
        read_obj(
            s3, "model-id-1/run-id-1/month/F1/regional/country/timeseries/qualifiers/qual2/q1/A.csv"
        ),
    )
    assert_csv_frame_equal(
        """timestamp,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        0,14.0,7.0,7.0,3.5,2
        1,46.0,23.0,19.0,9.5,2
        """,
        read_obj(
            s3, "model-id-1/run-id-1/month/F1/regional/country/timeseries/qualifiers/qual2/q2/A.csv"
        ),
    )

    # admin1 level
    assert_csv_frame_equal(
        """timestamp,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        0,8.0,4.0,4.0,2.0,2
        1,24.0,12.0,6.0,3.0,2
        """,
        read_obj(
            s3,
            "model-id-1/run-id-1/month/F1/regional/admin1/timeseries/qualifiers/qual1/qa/A__AA.csv",
        ),
    )
    assert_csv_frame_equal(
        """timestamp,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        0,4.0,4.0,2.0,2.0,1
        1,16.0,16.0,4.0,4.0,1
        """,
        read_obj(
            s3,
            "model-id-1/run-id-1/month/F1/regional/admin1/timeseries/qualifiers/qual2/q2/A__AA.csv",
        ),
    )

    # F2
    assert_csv_frame_equal(
        """timestamp,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        1,30.0,30.0,15.0,15.0,1
        """,
        read_obj(
            s3,
            "model-id-1/run-id-1/month/F2/regional/admin1/timeseries/qualifiers/qual1/qa/A__AB.csv",
        ),
    )


# TODO: Test qualifier cols and map
