import pandas as pd
import dask.dataframe as dd
from pandas.testing import assert_frame_equal

from prefect.engine.signals import SKIP

from ..utils import execute_prefect_task, ts
from flows.data_pipeline import subtile_aggregation, DEFAULT_PARTITIONS


def test_subtile_aggregation_skip():
    df = dd.from_pandas(pd.DataFrame({}), npartitions=DEFAULT_PARTITIONS)

    result = execute_prefect_task(subtile_aggregation)(df, "", True)
    assert type(result) == SKIP
    assert str(result) == "'subtile_aggregation' is skipped."


def test_subtile_aggregation():
    columns = ["timestamp", "country", "lat", "lng", "feature", "t_sum", "t_mean"]
    data = [
        # t1
        [0, "Ethiopia", 10.168, 40.646, "feature1", 4.0, 2.0],  # p1
        [0, "Ethiopia", 10.167, 40.645, "feature1", 12.0, 6.0],
        [0, "Ethiopia", 10.166, 40.644, "feature1", 80.0, 40.0],
        [0, "South Sudan", 9.55, 31.65, "feature1", 60.0, 30.0],  # p2
        [0, "South Sudan", 9.555, 31.655, "feature1", 60.0, 30.0],
        # t2
        [1, "Ethiopia", 10.168, 40.646, "feature1", 6.0, 3.0],  # p3
        [1, "Ethiopia", 10.167, 40.645, "feature1", 14.0, 7.0],
        [1, "Ethiopia", 10.166, 40.644, "feature1", 60.0, 30.0],
        [1, "South Sudan", 9.55, 31.65, "feature1", 80, 0.8],  # p4
        [1, "South Sudan", 9.555, 31.655, "feature1", 10, 1.0],
        # t2 feature 2
        [1, "South Sudan", 9.55, 31.65, "feature2", 180, 1.8],  # p5
    ]
    df = dd.from_pandas(pd.DataFrame(data, columns=columns), npartitions=DEFAULT_PARTITIONS)

    result = execute_prefect_task(subtile_aggregation)(df, "", False).compute()

    expected = pd.DataFrame(
        [
            ["feature1", 0, (14, 10041, 7726), 96.0, 48.0, 3],
            ["feature1", 0, (14, 9632, 7755), 120.0, 60.0, 2],
            ["feature1", 1, (14, 10041, 7726), 80.0, 40.0, 3],
            ["feature1", 1, (14, 9632, 7755), 90.0, 1.8, 2],
            ["feature2", 1, (14, 9632, 7755), 180.0, 1.8, 1],
        ],
        columns=["feature", "timestamp", "subtile", "s_sum_t_sum", "s_sum_t_mean", "s_count"],
    )

    assert_frame_equal(result, expected)
