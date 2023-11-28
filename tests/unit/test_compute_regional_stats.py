import boto3
import json
from moto import mock_s3

import pandas as pd
import dask.dataframe as dd
from prefect.engine.signals import SKIP

from ..utils import (
    execute_prefect_task,
    assert_json_equal,
    read_obj,
    S3_DEST,
)
from flows.data_pipeline import compute_regional_stats, DEFAULT_PARTITIONS


@mock_s3
def test_compute_global_timeseries_skip():
    df = dd.from_pandas(pd.DataFrame({}), npartitions=DEFAULT_PARTITIONS)

    result = execute_prefect_task(compute_regional_stats)(df, {}, "month", "mid1", "rid1", "", True)
    assert type(result) == SKIP
    assert str(result) == "'compute_regional_stats' is skipped."


@mock_s3
def test_compute_regional_stats():
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
        # t3
        [2, "A", "AA", "AAA", "AAAA", "F1", 20.0, 10.0],
        [2, "A", "AA", "AAA", "AAAA", "F1", 40.0, 20.0],
        [2, "A", "AA", "AAB", "AABD", "F1", 4.0, 2.0],
        [2, "A", "AA", "AAB", "AABE", "F1", 6.0, 2.0],
        [2, "B", "BA", "BAB", "BABB", "F1", 40.0, 10.0],
        [2, "B", "BB", "BBB", "BBBB", "F1", 300.0, 10.0],
        # feature 2
        [1, "A", "AA", "AAA", "AAAA", "F2", 4.0, 2.0],
        [2, "A", "AA", "AAA", "AAAB", "F2", 10.0, 5.0],
        [3, "A", "AA", "AAA", "AAAC", "F2", 5.0, 5.0],
    ]

    df = dd.from_pandas(pd.DataFrame(data, columns=columns), npartitions=DEFAULT_PARTITIONS)
    execute_prefect_task(compute_regional_stats)(df, S3_DEST, "month", "model-id-1", "run-id-1", "")

    assert_json_equal(
        """
        {"min": {"s_sum_t_sum": [{"region_id": "A", "timestamp": 1, "value": 24.0}], "s_mean_t_sum": [{"region_id": "A", "timestamp": 1, "value": 6.0}], "s_sum_t_mean": [{"region_id": "A", "timestamp": 1, "value": 6.0}], "s_mean_t_mean": [{"region_id": "A", "timestamp": 1, "value": 1.5}]}, "max": {"s_sum_t_sum": [{"region_id": "B", "timestamp": 2, "value": 340.0}], "s_mean_t_sum": [{"region_id": "B", "timestamp": 2, "value": 170.0}], "s_sum_t_mean": [{"region_id": "B", "timestamp": 1, "value": 50.0}], "s_mean_t_mean": [{"region_id": "B", "timestamp": 1, "value": 25.0}]}}
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F1/regional/country/stats/default/extrema.json"),
    )

    assert_json_equal(
        """
        {"min": {"s_sum_t_sum": [{"region_id": "B__BB", "timestamp": 0, "value": 10.0}], "s_mean_t_sum": [{"region_id": "A__AA", "timestamp": 1, "value": 6.0}], "s_sum_t_mean": [{"region_id": "B__BB", "timestamp": 0, "value": 2.0}], "s_mean_t_mean": [{"region_id": "A__AA", "timestamp": 1, "value": 1.5}]}, "max": {"s_sum_t_sum": [{"region_id": "B__BB", "timestamp": 2, "value": 300.0}], "s_mean_t_sum": [{"region_id": "B__BB", "timestamp": 2, "value": 300.0}], "s_sum_t_mean": [{"region_id": "A__AA", "timestamp": 2, "value": 34.0}], "s_mean_t_mean": [{"region_id": "B__BA", "timestamp": 1, "value": 30.0}, {"region_id": "B__BA", "timestamp": 0, "value": 30.0}]}}
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F1/regional/admin1/stats/default/extrema.json"),
    )

    assert_json_equal(
        """
        {"min": {"s_sum_t_sum": [{"region_id": "A__AA__AAA", "timestamp": 1, "value": 6.0}], "s_mean_t_sum": [{"region_id": "A__AA__AAA", "timestamp": 1, "value": 3.0}], "s_sum_t_mean": [{"region_id": "A__AA__AAA", "timestamp": 1, "value": 2.0}, {"region_id": "B__BB__BBB", "timestamp": 0, "value": 2.0}], "s_mean_t_mean": [{"region_id": "A__AA__AAA", "timestamp": 1, "value": 1.0}]}, "max": {"s_sum_t_sum": [{"region_id": "B__BB__BBB", "timestamp": 2, "value": 300.0}], "s_mean_t_sum": [{"region_id": "B__BB__BBB", "timestamp": 2, "value": 300.0}], "s_sum_t_mean": [{"region_id": "A__AA__AAA", "timestamp": 2, "value": 30.0}, {"region_id": "B__BA__BAB", "timestamp": 1, "value": 30.0}, {"region_id": "B__BA__BAB", "timestamp": 0, "value": 30.0}], "s_mean_t_mean": [{"region_id": "B__BA__BAB", "timestamp": 1, "value": 30.0}, {"region_id": "B__BA__BAB", "timestamp": 0, "value": 30.0}]}}
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F1/regional/admin2/stats/default/extrema.json"),
    )

    assert_json_equal(
        """
        {"min": {"s_sum_t_sum": [{"region_id": "A__AA__AAB__AABE", "timestamp": 0, "value": 3.0}], "s_mean_t_sum": [{"region_id": "A__AA__AAA__AAAA", "timestamp": 1, "value": 3.0}, {"region_id": "A__AA__AAB__AABE", "timestamp": 0, "value": 3.0}], "s_sum_t_mean": [{"region_id": "A__AA__AAB__AABE", "timestamp": 0, "value": 1.0}], "s_mean_t_mean": [{"region_id": "A__AA__AAA__AAAA", "timestamp": 1, "value": 1.0}, {"region_id": "A__AA__AAB__AABE", "timestamp": 0, "value": 1.0}]}, "max": {"s_sum_t_sum": [{"region_id": "B__BB__BBB__BBBB", "timestamp": 2, "value": 300.0}], "s_mean_t_sum": [{"region_id": "B__BB__BBB__BBBB", "timestamp": 2, "value": 300.0}], "s_sum_t_mean": [{"region_id": "A__AA__AAA__AAAA", "timestamp": 2, "value": 30.0}, {"region_id": "B__BA__BAB__BABB", "timestamp": 1, "value": 30.0}, {"region_id": "B__BA__BAB__BABB", "timestamp": 0, "value": 30.0}], "s_mean_t_mean": [{"region_id": "B__BA__BAB__BABB", "timestamp": 1, "value": 30.0}, {"region_id": "B__BA__BAB__BABB", "timestamp": 0, "value": 30.0}]}}
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F1/regional/admin3/stats/default/extrema.json"),
    )

    assert_json_equal(
        """
        {"min": {"s_sum_t_sum": [{"region_id": "A__AA__AAB__AABE", "timestamp": 0, "value": 3.0}], "s_mean_t_sum": [{"region_id": "A__AA__AAA__AAAA", "timestamp": 1, "value": 3.0}, {"region_id": "A__AA__AAB__AABE", "timestamp": 0, "value": 3.0}], "s_sum_t_mean": [{"region_id": "A__AA__AAB__AABE", "timestamp": 0, "value": 1.0}], "s_mean_t_mean": [{"region_id": "A__AA__AAA__AAAA", "timestamp": 1, "value": 1.0}, {"region_id": "A__AA__AAB__AABE", "timestamp": 0, "value": 1.0}]}, "max": {"s_sum_t_sum": [{"region_id": "B__BB__BBB__BBBB", "timestamp": 2, "value": 300.0}], "s_mean_t_sum": [{"region_id": "B__BB__BBB__BBBB", "timestamp": 2, "value": 300.0}], "s_sum_t_mean": [{"region_id": "A__AA__AAA__AAAA", "timestamp": 2, "value": 30.0}, {"region_id": "B__BA__BAB__BABB", "timestamp": 1, "value": 30.0}, {"region_id": "B__BA__BAB__BABB", "timestamp": 0, "value": 30.0}], "s_mean_t_mean": [{"region_id": "B__BA__BAB__BABB", "timestamp": 1, "value": 30.0}, {"region_id": "B__BA__BAB__BABB", "timestamp": 0, "value": 30.0}]}}
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F1/regional/admin3/stats/default/extrema.json"),
    )

    assert_json_equal(
        """
        {"min": {"s_sum_t_sum": [{"region_id": "A__AA__AAA__AAAA", "timestamp": 1, "value": 4.0}], "s_mean_t_sum": [{"region_id": "A__AA__AAA__AAAA", "timestamp": 1, "value": 4.0}], "s_sum_t_mean": [{"region_id": "A__AA__AAA__AAAA", "timestamp": 1, "value": 2.0}], "s_mean_t_mean": [{"region_id": "A__AA__AAA__AAAA", "timestamp": 1, "value": 2.0}]}, "max": {"s_sum_t_sum": [{"region_id": "A__AA__AAA__AAAB", "timestamp": 2, "value": 10.0}], "s_mean_t_sum": [{"region_id": "A__AA__AAA__AAAB", "timestamp": 2, "value": 10.0}], "s_sum_t_mean": [{"region_id": "A__AA__AAA__AAAC", "timestamp": 3, "value": 5.0}, {"region_id": "A__AA__AAA__AAAB", "timestamp": 2, "value": 5.0}], "s_mean_t_mean": [{"region_id": "A__AA__AAA__AAAC", "timestamp": 3, "value": 5.0}, {"region_id": "A__AA__AAA__AAAB", "timestamp": 2, "value": 5.0}]}}
        """,
        read_obj(s3, "model-id-1/run-id-1/month/F2/regional/admin3/stats/default/extrema.json"),
    )


@mock_s3
def test_compute_regional_stats_multiple_extrema():
    # connect to mock s3 storage
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket=S3_DEST["bucket"])

    columns = ["timestamp", "country", "feature", "t_sum", "t_mean"]
    data = [
        [1, "A", "F1", 20.0, 4.0],
        [2, "A", "F1", 10.0, 5.0],
        [3, "A", "F1", 10.0, 10.0],
        [1, "B", "F1", 20.0, 5.0],
        [2, "B", "F1", 20.0, 4.0],
        [3, "B", "F1", 10.0, 2.0],
        # F2
        [1, "A", "F2", 10.0, 2.0],
        [2, "A", "F2", 10.0, 2.0],
        [3, "A", "F2", 10.0, 2.0],
        [4, "A", "F2", 10.0, 2.0],
        [5, "A", "F2", 10.0, 2.0],
        [6, "A", "F2", 10.0, 2.0],
        [7, "A", "F2", 10.0, 2.0],
        [8, "A", "F2", 10.0, 2.0],
        [9, "A", "F2", 10.0, 2.0],
        [10, "A", "F2", 10.0, 2.0],
        [11, "A", "F2", 10.0, 2.0],
        [12, "A", "F2", 10.0, 2.0],
        [13, "A", "F2", 10.0, 2.0],
        [14, "A", "F2", 10.0, 2.0],
        [15, "A", "F2", 10.0, 2.0],
        [16, "A", "F2", 10.0, 2.0],
        [17, "A", "F2", 10.0, 2.0],
        [18, "A", "F2", 10.0, 2.0],
        [19, "A", "F2", 10.0, 2.0],
        [20, "A", "F2", 10.0, 2.0],
        [21, "A", "F2", 10.0, 2.0],
        [22, "A", "F2", 10.0, 2.0],
    ]

    df = dd.from_pandas(pd.DataFrame(data, columns=columns), npartitions=DEFAULT_PARTITIONS)
    execute_prefect_task(compute_regional_stats)(df, S3_DEST, "year", "model-id-2", "run-id-2", "")

    assert_json_equal(
        """
        {"min": {"s_sum_t_sum": [{"region_id": "A", "timestamp": 3, "value": 10.0}, {"region_id": "B", "timestamp": 3, "value": 10.0}, {"region_id": "A", "timestamp": 2, "value": 10.0}], "s_mean_t_sum": [{"region_id": "A", "timestamp": 3, "value": 10.0}, {"region_id": "B", "timestamp": 3, "value": 10.0}, {"region_id": "A", "timestamp": 2, "value": 10.0}], "s_sum_t_mean": [{"region_id": "B", "timestamp": 3, "value": 2.0}], "s_mean_t_mean": [{"region_id": "B", "timestamp": 3, "value": 2.0}]}, "max": {"s_sum_t_sum": [{"region_id": "B", "timestamp": 2, "value": 20.0}, {"region_id": "A", "timestamp": 1, "value": 20.0}, {"region_id": "B", "timestamp": 1, "value": 20.0}], "s_mean_t_sum": [{"region_id": "B", "timestamp": 2, "value": 20.0}, {"region_id": "A", "timestamp": 1, "value": 20.0}, {"region_id": "B", "timestamp": 1, "value": 20.0}], "s_sum_t_mean": [{"region_id": "A", "timestamp": 3, "value": 10.0}], "s_mean_t_mean": [{"region_id": "A", "timestamp": 3, "value": 10.0}]}}
        """,
        read_obj(s3, "model-id-2/run-id-2/year/F1/regional/country/stats/default/extrema.json"),
    )

    # should have max number of items per aggregation type equal to 20
    max_num_items = 20  # we expect to have the limit on the # of each items for each aggregation type to prevent the list growing indefinitely
    r = json.loads(
        read_obj(s3, "model-id-2/run-id-2/year/F2/regional/country/stats/default/extrema.json")
    )
    assert len(r["min"]["s_sum_t_sum"]) == max_num_items
    assert len(r["min"]["s_mean_t_sum"]) == max_num_items
    assert len(r["min"]["s_sum_t_mean"]) == max_num_items
    assert len(r["min"]["s_mean_t_mean"]) == max_num_items
    assert len(r["max"]["s_sum_t_sum"]) == max_num_items
    assert len(r["max"]["s_mean_t_sum"]) == max_num_items
    assert len(r["max"]["s_sum_t_mean"]) == max_num_items
    assert len(r["max"]["s_mean_t_mean"]) == max_num_items
