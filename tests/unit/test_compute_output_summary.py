import pandas as pd
import dask.dataframe as dd

from ..utils import execute_prefect_task
from flows.data_pipeline import compute_output_summary, DEFAULT_PARTITIONS


def test_compute_output_summary():
    columns = ["timestamp", "country", "lat", "lng", "feature", "qual1", "t_sum", "t_mean"]
    data = [
        [0, "A", 1.1, 1.0, "Feature1", "a", 6.3, 2.1],
        [0, "B", 2.1, 2.0, "Feature1", "a", 12.0, 4.0],
        [0, "A", 1.1, 1.0, "Feature1", "b", 3.3, 1.1],
        [0, "B", 2.1, 2.0, "Feature1", "b", 10.2, 3.4],
        [0, "A", 1.1, 1.0, "Feature2", "a", 8.3, 4.1],
        [0, "B", 2.1, 2.0, "Feature2", "a", 14.0, 3.0],
        [0, "A", 1.1, 1.0, "Feature2", "b", 6.3, 2.1],
        [0, "B", 2.1, 2.0, "Feature2", "b", 20.2, 5.4],
    ]
    df = dd.from_pandas(pd.DataFrame(data, columns=columns), npartitions=DEFAULT_PARTITIONS)

    result = execute_prefect_task(compute_output_summary)(df, "")

    assert [
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
    ] == sorted(result, key=lambda x: x["name"])
