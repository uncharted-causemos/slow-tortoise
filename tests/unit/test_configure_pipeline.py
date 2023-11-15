import pandas as pd
import dask.dataframe as dd
from unittest.mock import patch

from ..utils import execute_prefect_task
from flows.data_pipeline import configure_pipeline, DEFAULT_PARTITIONS

data = {
    "timestamp": [4, 5, 6],
    "country": ["United States", "Canada", "Canada"],
    "admin1": ["New York", "Ontario", "Quebec"],
    "admin2": ["nyadmi2", "onadmin2", "qadmin2"],
    "admin3": ["nyadmin3", "onadmin3", "qadmin3"],
    "lat": [1.1, 2.2, 3.2],
    "lng": [3.3, 2.2, 1.1],
    "feature": ["A", "A", "B"],
    "value": [6.2, 7.1, 9.2],
    "qual1": ["d", None, "f"],
    "qual2": ["q2a", "q2b", "q2c"],
}


def test_configure_pipeline():
    df = dd.from_pandas(pd.DataFrame(data, index=[0, 1, 2]), npartitions=DEFAULT_PARTITIONS)
    dest = {"key": "key", "secret": "secret"}
    (dest, compute_monthly, compute_annual, compute_summary, skipped_tasks) = execute_prefect_task(
        configure_pipeline
    )(df, dest, "indicator_bucket", "model_bucket", False, None)

    assert dest == {"key": "key", "secret": "secret", "bucket": "model_bucket"}
    assert compute_monthly
    assert compute_annual
    assert compute_summary
    assert skipped_tasks == {
        "compute_global_timeseries": False,
        "compute_regional_stats": False,
        "compute_regional_timeseries": False,
        "compute_regional_aggregation": False,
        "compute_tiles": False,
        "record_results": False,
    }


def test_configure_pipeline_indicator():
    df = dd.from_pandas(pd.DataFrame(data, index=[0, 1, 2]), npartitions=DEFAULT_PARTITIONS)
    dest = {"key": "key", "secret": "secret"}
    (dest, compute_monthly, compute_annual, compute_summary, skipped_tasks) = execute_prefect_task(
        configure_pipeline
    )(df, dest, "indicator_bucket", "model_bucket", True, None)

    assert dest == {"key": "key", "secret": "secret", "bucket": "indicator_bucket"}
    assert compute_monthly
    assert compute_annual
    assert not compute_summary
    assert skipped_tasks == {
        "compute_global_timeseries": False,
        "compute_regional_stats": False,
        "compute_regional_timeseries": False,
        "compute_regional_aggregation": False,
        "compute_tiles": False,
        "record_results": False,
    }


def test_configure_pipeline_no_geo_coord():
    data = {
        "timestamp": [4, 5, 6],
        "country": ["United States", "Canada", "Canada"],
        "admin1": ["New York", "Ontario", "Quebec"],
        "admin2": ["nyadmi2", "onadmin2", "qadmin2"],
        "admin3": ["nyadmin3", "onadmin3", "qadmin3"],
        "feature": ["A", "A", "B"],
        "value": [6.2, 7.1, 9.2],
        "qual1": ["d", None, "f"],
        "qual2": ["q2a", "q2b", "q2c"],
    }
    df = dd.from_pandas(pd.DataFrame(data, index=[0, 1, 2]), npartitions=DEFAULT_PARTITIONS)
    dest = {"key": "key", "secret": "secret"}
    (_, _, _, _, skipped_tasks) = execute_prefect_task(configure_pipeline)(
        df, dest, "indicator_bucket", "model_bucket", True, None
    )

    assert skipped_tasks == {
        "compute_global_timeseries": False,
        "compute_regional_stats": False,
        "compute_regional_timeseries": False,
        "compute_regional_aggregation": False,
        "compute_tiles": True,
        "record_results": False,
    }


def test_configure_pipeline_selected_tasks():
    df = dd.from_pandas(pd.DataFrame(data, index=[0, 1, 2]), npartitions=DEFAULT_PARTITIONS)
    dest = {"key": "key", "secret": "secret"}
    selected_tasks = ["compute_global_timeseries", "compute_regional_aggregation", "invalid_task"]
    (_, _, _, _, skipped_tasks) = execute_prefect_task(configure_pipeline)(
        df, dest, "indicator_bucket", "model_bucket", True, selected_tasks
    )

    assert skipped_tasks == {
        "compute_global_timeseries": False,
        "compute_regional_stats": True,
        "compute_regional_timeseries": True,
        "compute_regional_aggregation": False,
        "compute_tiles": True,
        "record_results": True,
    }
