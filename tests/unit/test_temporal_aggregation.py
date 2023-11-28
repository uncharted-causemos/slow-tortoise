import pandas as pd
import dask.dataframe as dd
from prefect.engine.signals import SKIP
from pandas.testing import assert_frame_equal

from ..utils import execute_prefect_task, ts
from flows.data_pipeline import temporal_aggregation, DEFAULT_PARTITIONS


def test_temporal_aggregation_skip():
    df = dd.from_pandas(pd.DataFrame({}), npartitions=DEFAULT_PARTITIONS)

    result = execute_prefect_task(temporal_aggregation)(df, "month", False, "")
    assert type(result) == SKIP
    assert str(result) == "Aggregating for resolution month was not requested"


def test_temporal_aggregation_monthly():
    columns = ["timestamp", "country", "lat", "lng", "feature", "value", "qual1"]
    data = [
        [ts("2022-01-01"), "A", 1.1, 1.0, "Feature1", 1.0, "a"],
        [ts("2022-01-15"), "A", 1.1, 1.0, "Feature1", 3.0, "a"],
        [ts("2022-02-02"), "A", 1.1, 1.0, "Feature1", 1.1, "a"],
        [ts("2022-01-01"), "B", 2.1, 2.0, "Feature1", 4.0, "a"],
        [ts("2022-01-15"), "B", 2.1, 2.0, "Feature1", 6.0, "a"],
        [ts("2022-02-02"), "B", 2.1, 2.0, "Feature1", 2.0, "a"],
        [ts("2022-01-01"), "A", 1.1, 1.0, "Feature2", 1.0, "a"],
        [ts("2022-01-15"), "A", 1.1, 1.0, "Feature2", 1.0, "a"],
        [ts("2022-02-02"), "A", 1.1, 1.0, "Feature2", 1.2, "a"],
        [ts("2022-01-01"), "B", 2.1, 2.0, "Feature2", 3.0, "a"],
        [ts("2022-01-15"), "B", 2.1, 2.0, "Feature2", 3.0, "a"],
        [ts("2022-02-02"), "B", 2.1, 2.0, "Feature2", 4.2, "a"],
    ]
    df = dd.from_pandas(pd.DataFrame(data, columns=columns), npartitions=DEFAULT_PARTITIONS)

    result = execute_prefect_task(temporal_aggregation)(df, "month", True, "").compute()

    expected = pd.DataFrame(
        [
            [ts("2022-01-01"), "A", 1.1, 1.0, "Feature1", "a", 4.0, 2.0],
            [ts("2022-02-01"), "A", 1.1, 1.0, "Feature1", "a", 1.1, 1.1],
            [ts("2022-01-01"), "B", 2.1, 2.0, "Feature1", "a", 10.0, 5.0],
            [ts("2022-02-01"), "B", 2.1, 2.0, "Feature1", "a", 2.0, 2.0],
            [ts("2022-01-01"), "A", 1.1, 1.0, "Feature2", "a", 2.0, 1.0],
            [ts("2022-02-01"), "A", 1.1, 1.0, "Feature2", "a", 1.2, 1.2],
            [ts("2022-01-01"), "B", 2.1, 2.0, "Feature2", "a", 6.0, 3.0],
            [ts("2022-02-01"), "B", 2.1, 2.0, "Feature2", "a", 4.2, 4.2],
        ],
        columns=["timestamp", "country", "lat", "lng", "feature", "qual1", "t_sum", "t_mean"],
    )

    assert_frame_equal(result, expected)


def test_temporal_aggregation_yearly():
    columns = ["timestamp", "country", "lat", "lng", "feature", "value", "qual1"]
    data = [
        [ts("2022-01-01"), "A", 1.1, 1.0, "Feature1", 1.0, "a"],
        [ts("2022-01-15"), "A", 1.1, 1.0, "Feature1", 3.0, "a"],
        [ts("2022-02-02"), "A", 1.1, 1.0, "Feature1", 2.3, "a"],
        [ts("2022-01-01"), "B", 2.1, 2.0, "Feature1", 4.0, "a"],
        [ts("2022-01-15"), "B", 2.1, 2.0, "Feature1", 6.0, "a"],
        [ts("2022-02-02"), "B", 2.1, 2.0, "Feature1", 2.0, "a"],
        [ts("2022-01-01"), "A", 1.1, 1.0, "Feature1", 1.1, "b"],
        [ts("2022-01-15"), "A", 1.1, 1.0, "Feature1", 1.0, "b"],
        [ts("2022-02-02"), "A", 1.1, 1.0, "Feature1", 1.2, "b"],
        [ts("2022-01-01"), "B", 2.1, 2.0, "Feature1", 3.0, "b"],
        [ts("2022-01-15"), "B", 2.1, 2.0, "Feature1", 3.0, "b"],
        [ts("2022-02-02"), "B", 2.1, 2.0, "Feature1", 4.2, "b"],
    ]
    df = dd.from_pandas(pd.DataFrame(data, columns=columns), npartitions=DEFAULT_PARTITIONS)

    result = execute_prefect_task(temporal_aggregation)(df, "year", True, "").compute()

    expected = pd.DataFrame(
        [
            [ts("2022-01-01"), "A", 1.1, 1.0, "Feature1", "a", 6.3, 2.1],
            [ts("2022-01-01"), "B", 2.1, 2.0, "Feature1", "a", 12.0, 4.0],
            [ts("2022-01-01"), "A", 1.1, 1.0, "Feature1", "b", 3.3, 1.1],
            [ts("2022-01-01"), "B", 2.1, 2.0, "Feature1", "b", 10.2, 3.4],
        ],
        columns=["timestamp", "country", "lat", "lng", "feature", "qual1", "t_sum", "t_mean"],
    )

    assert_frame_equal(result, expected)


def test_temporal_aggregation_all():
    columns = ["timestamp", "country", "lat", "lng", "feature", "value", "qual1"]
    data = [
        [ts("2022-01-01"), "A", 1.1, 1.0, "Feature1", 1.0, "a"],
        [ts("2022-01-15"), "A", 1.1, 1.0, "Feature1", 3.0, "a"],
        [ts("2022-02-02"), "A", 1.1, 1.0, "Feature1", 2.3, "a"],
        [ts("2022-01-01"), "B", 2.1, 2.0, "Feature1", 4.0, "a"],
        [ts("2022-01-15"), "B", 2.1, 2.0, "Feature1", 6.0, "a"],
        [ts("2022-02-02"), "B", 2.1, 2.0, "Feature1", 2.0, "a"],
        [ts("2022-01-01"), "A", 1.1, 1.0, "Feature1", 1.1, "b"],
        [ts("2022-01-15"), "A", 1.1, 1.0, "Feature1", 1.0, "b"],
        [ts("2022-02-02"), "A", 1.1, 1.0, "Feature1", 1.2, "b"],
        [ts("2022-01-01"), "B", 2.1, 2.0, "Feature1", 3.0, "b"],
        [ts("2022-01-15"), "B", 2.1, 2.0, "Feature1", 3.0, "b"],
        [ts("2022-02-02"), "B", 2.1, 2.0, "Feature1", 4.2, "b"],
    ]
    df = dd.from_pandas(pd.DataFrame(data, columns=columns), npartitions=DEFAULT_PARTITIONS)

    result = execute_prefect_task(temporal_aggregation)(df, "all", True, "").compute()

    expected = pd.DataFrame(
        [
            [0, "A", 1.1, 1.0, "Feature1", "a", 6.3, 2.1],
            [0, "B", 2.1, 2.0, "Feature1", "a", 12.0, 4.0],
            [0, "A", 1.1, 1.0, "Feature1", "b", 3.3, 1.1],
            [0, "B", 2.1, 2.0, "Feature1", "b", 10.2, 3.4],
        ],
        columns=["timestamp", "country", "lat", "lng", "feature", "qual1", "t_sum", "t_mean"],
    )
    assert_frame_equal(result, expected)
