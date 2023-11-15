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
from flows.data_pipeline import compute_regional_aggregation, DEFAULT_PARTITIONS


@mock_s3
def test_compute_global_timeseries_skip():
    df = dd.from_pandas(pd.DataFrame({}), npartitions=DEFAULT_PARTITIONS)

    result = execute_prefect_task(compute_regional_aggregation)(
        df, {}, "month", "mid1", "rid1", {}, [], "", True
    )
    assert type(result) == SKIP
    assert str(result) == "'compute_regional_aggregation' is skipped."


@mock_s3
def test_compute_regional_aggregation():
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

    df = dd.from_pandas(pd.DataFrame(data, columns=columns), npartitions=DEFAULT_PARTITIONS)
    execute_prefect_task(compute_regional_aggregation)(
        df, S3_DEST, "month", "model-id-1", "run-id-1", {}, [], ""
    )

    assert_csv_frame_equal(
        """id,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        A,25.0,6.25,12.0,3.0,4
        B,70.0,35.0,32.0,16.0,2
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F1/regional/country/aggs/0/default/default.csv"),
    )
    assert_csv_frame_equal(
        """id,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        A,24.0,6.0,6.0,1.5,4
        B,170.0,85.0,50.0,25.0,2
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F1/regional/country/aggs/1/default/default.csv"),
    )
    assert_csv_frame_equal(
        """id,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        A,14.0,7.0,7.0,3.5,2
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F2/regional/country/aggs/1/default/default.csv"),
    )

    assert_csv_frame_equal(
        """id,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        A__AA,24.0,6.0,6.0,1.5,4
        B__BA,90.0,90.0,30.0,30.0,1
        B__BB,80.0,80.0,20.0,20.0,1
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F1/regional/admin1/aggs/1/default/default.csv"),
    )
    assert_csv_frame_equal(
        """id,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        A__AA,25.0,6.25,12.0,3.0,4
        B__BA,60.0,60.0,30.0,30.0,1
        B__BB,10.0,10.0,2.0,2.0,1
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F1/regional/admin1/aggs/0/default/default.csv"),
    )
    assert_csv_frame_equal(
        """id,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        A__AA__AAB,18.0,9.0,4.0,2.0,2
        B__BA__BAB,90.0,90.0,30.0,30.0,1
        A__AA__AAA,6.0,3.0,2.0,1.0,2
        B__BB__BBB,80.0,80.0,20.0,20.0,1
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F1/regional/admin2/aggs/1/default/default.csv"),
    )
    assert_csv_frame_equal(
        """id,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        A__AA__AAB__AABD,8.0,8.0,4.0,4.0,1
        A__AA__AAA__AAAA,14.0,7.0,7.0,3.5,2
        B__BA__BAB__BABB,60.0,60.0,30.0,30.0,1
        A__AA__AAB__AABE,3.0,3.0,1.0,1.0,1
        B__BB__BBB__BBBB,10.0,10.0,2.0,2.0,1
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F1/regional/admin3/aggs/0/default/default.csv"),
    )


@mock_s3
def test_compute_regional_aggregation_with_qualifiers():
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
    qual_cols = [["qual1"], ["qual2"]]
    qual_map = {"F1": ["qual1", "qual2"], "F2": ["qual1", "qual2"]}
    df = dd.from_pandas(pd.DataFrame(data, columns=columns), npartitions=DEFAULT_PARTITIONS)

    execute_prefect_task(compute_regional_aggregation)(
        df,
        S3_DEST,
        "year",
        "model-id-q",
        "run-id-q",
        qual_map,
        qual_cols,
        "",
    )

    # should still produce regional aggregation without qualifier
    assert_csv_frame_equal(
        """id,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        A__AA,24.0,12.0,6.0,3.0,2
        A__AB,50.0,25.0,25.0,12.5,2
        """,
        read_obj(s3, "model-id-q/run-id-q/year/F1/regional/admin1/aggs/1/default/default.csv"),
    )

    # output for country level by qualifier
    assert_csv_frame_equal(
        """id,qualifier,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        A,qa,28.0,7.0,14.0,3.5,4
        """,
        read_obj(s3, "model-id-q/run-id-q/year/F1/regional/country/aggs/0/qualifiers/qual1.csv"),
        sort_by=["id", "qualifier"],
    )
    assert_csv_frame_equal(
        """id,qualifier,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        A,qa,74.0,18.5,31.0,7.75,4
        """,
        read_obj(s3, "model-id-q/run-id-q/year/F1/regional/country/aggs/1/qualifiers/qual1.csv"),
        sort_by=["id", "qualifier"],
    )
    assert_csv_frame_equal(
        """id,qualifier,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        A,q1,14.0,7.0,7.0,3.5,2
        A,q2,14.0,7.0,7.0,3.5,2
        """,
        read_obj(s3, "model-id-q/run-id-q/year/F1/regional/country/aggs/0/qualifiers/qual2.csv"),
        sort_by=["id", "qualifier"],
    )

    # admin1 level
    assert_csv_frame_equal(
        """id,qualifier,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        A__AA,qa,8.0,4.0,4.0,2.0,2
        A__AB,qa,20.0,10.0,10.0,5.0,2
        """,
        read_obj(
            s3,
            "model-id-q/run-id-q/year/F1/regional/admin1/aggs/0/qualifiers/qual1.csv",
        ),
        sort_by=["id", "qualifier"],
    )
    assert_csv_frame_equal(
        """id,qualifier,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        A__AA,q1,8.0,8.0,2.0,2.0,1
        A__AA,q2,16.0,16.0,4.0,4.0,1
        A__AB,q2,30.0,30.0,15.0,15.0,1
        A__AB,q1,20.0,20.0,10.0,10.0,1
        """,
        read_obj(
            s3,
            "model-id-q/run-id-q/year/F1/regional/admin1/aggs/1/qualifiers/qual2.csv",
        ),
        sort_by=["id", "qualifier"],
    )

    # F2
    assert_csv_frame_equal(
        """id,qualifier,s_sum_t_sum,s_mean_t_sum,s_sum_t_mean,s_mean_t_mean,s_count
        A,qa,30.0,30.0,15.0,15.0,1
        """,
        read_obj(s3, "model-id-q/run-id-q/year/F2/regional/country/aggs/1/qualifiers/qual1.csv"),
        sort_by=["id", "qualifier"],
    )


# # TODO: Test qualifier cols and map
