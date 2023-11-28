import pandas as pd
import dask.dataframe as dd

import boto3
from moto import mock_s3

from ..utils import execute_prefect_task, assert_json_equal, read_obj, S3_DEST
from flows.data_pipeline import record_results, DEFAULT_PARTITIONS
from prefect.engine.signals import SKIP


@mock_s3
def test_compute_output_summary_skip():
    # connect to mock s3 storage
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket=S3_DEST["bucket"])

    summary_values = [
        {
            "name": "Feature1",
            "s_min_t_sum": 3.3,
            "s_max_t_sum": 12.0,
            "s_sum_t_sum": 31.8,
            "s_mean_t_sum": 7.95,
            "s_min_t_mean": 1.1,
            "s_max_t_mean": 4.0,
            "s_sum_t_mean": 10.6,
            "s_mean_t_mean": 2.65,
        },
        {
            "name": "Feature2",
            "s_min_t_sum": 6.3,
            "s_max_t_sum": 20.2,
            "s_sum_t_sum": 48.8,
            "s_mean_t_sum": 12.2,
            "s_min_t_mean": 2.1,
            "s_max_t_mean": 5.4,
            "s_sum_t_mean": 14.6,
            "s_mean_t_mean": 3.65,
        },
    ]

    result = execute_prefect_task(record_results)(
        S3_DEST,
        "mid-1",
        "rid-1",
        summary_values,
        120,
        "",
        ["country", "admin1"],
        ["f1", "f2"],
        30,
        True,
        True,
        {"compute_tiles": False, "record_results": True},
        "",
        "",
        5,
        10,
        1,
        "",
    )
    assert type(result) == SKIP
    assert (
        str(result)
        == "'record_results' is skipped. 'record_results' task only runs when the pipeline is running with all output tasks."
    )


@mock_s3
def test_compute_output_summary():
    # connect to mock s3 storage
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket=S3_DEST["bucket"])

    summary_values = [
        {
            "name": "Feature1",
            "s_min_t_sum": 3.3,
            "s_max_t_sum": 12.0,
            "s_sum_t_sum": 31.8,
            "s_mean_t_sum": 7.95,
            "s_min_t_mean": 1.1,
            "s_max_t_mean": 4.0,
            "s_sum_t_mean": 10.6,
            "s_mean_t_mean": 2.65,
        },
        {
            "name": "Feature2",
            "s_min_t_sum": 6.3,
            "s_max_t_sum": 20.2,
            "s_sum_t_sum": 48.8,
            "s_mean_t_sum": 12.2,
            "s_min_t_mean": 2.1,
            "s_max_t_mean": 5.4,
            "s_sum_t_mean": 14.6,
            "s_mean_t_mean": 3.65,
        },
    ]

    execute_prefect_task(record_results)(
        S3_DEST,
        "mid-1",
        "rid-1",
        summary_values,
        120,
        '{"f1": 60, "f2": 60}',
        ["country", "admin1"],
        ["f1", "f2"],
        30,
        True,
        True,
        {"compute_tiles": False, "record_results": False},
        '{"f1": 20, "f2": 30}',
        '{"f1": 10, "f2": 10}',
        5,
        10,
        1,
        "",
    )

    assert_json_equal(
        '{"data_info": {"num_rows": 120, "num_rows_per_feature": {"f1": 60, "f2": 60}, "num_missing_ts": 5, "num_invalid_ts": 10, "num_missing_val": 1, "region_levels": ["country", "admin1", "grid data"], "features": ["f1", "f2"], "raw_count_threshold": 30, "has_tiles": true, "has_monthly": true, "has_annual": true, "has_weights": false, "month_timeseries_size": {"f1": 20, "f2": 30}, "year_timeseries_size": {"f1": 10, "f2": 10}}, "output_agg_values": [{"name": "Feature1", "s_min_t_sum": 3.3, "s_max_t_sum": 12.0, "s_sum_t_sum": 31.8, "s_mean_t_sum": 7.95, "s_min_t_mean": 1.1, "s_max_t_mean": 4.0, "s_sum_t_mean": 10.6, "s_mean_t_mean": 2.65}, {"name": "Feature2", "s_min_t_sum": 6.3, "s_max_t_sum": 20.2, "s_sum_t_sum": 48.8, "s_mean_t_sum": 12.2, "s_min_t_mean": 2.1, "s_max_t_mean": 5.4, "s_sum_t_mean": 14.6, "s_mean_t_mean": 3.65}]}',
        read_obj(s3, "mid-1/rid-1/results/results.json"),
    )


@mock_s3
def test_compute_output_summary_missing_info():
    # connect to mock s3 storage
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket=S3_DEST["bucket"])

    execute_prefect_task(record_results)(
        S3_DEST,
        "mid-1",
        "rid-1",
        None,
        120,
        '{"f1": 60, "f2": 60}',
        ["country", "admin1"],
        ["f1", "f2"],
        30,
        False,
        True,
        {"compute_tiles": True, "record_results": False},
        '{"f1": 20, "f2": 30}',
        None,
        5,
        10,
        1,
        "",
    )

    assert_json_equal(
        '{"data_info": {"num_rows": 120, "num_rows_per_feature": {"f1": 60, "f2": 60}, "num_missing_ts": 5, "num_invalid_ts": 10, "num_missing_val": 1, "region_levels": ["country", "admin1"], "features": ["f1", "f2"], "raw_count_threshold": 30, "has_tiles": false, "has_monthly": false, "has_annual": true, "has_weights": false}}',
        read_obj(s3, "mid-1/rid-1/results/results.json"),
    )
