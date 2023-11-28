import pandas as pd
import dask.dataframe as dd

from ..utils import execute_prefect_task
from flows.data_pipeline import get_qualifier_columns, DEFAULT_PARTITIONS

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


def test_get_qualifier_columns():
    df = dd.from_pandas(pd.DataFrame(data, index=[0, 1, 2]), npartitions=DEFAULT_PARTITIONS)

    cols = execute_prefect_task(get_qualifier_columns)(df, "")

    assert sorted(cols, key=lambda x: x[0]) == [["qual1"], ["qual2"]]


def test_get_qualifier_columns_ignore_weight_column():
    df = dd.from_pandas(pd.DataFrame(data, index=[0, 1, 2]), npartitions=DEFAULT_PARTITIONS)

    cols = execute_prefect_task(get_qualifier_columns)(df, "qual1")

    assert cols == [["qual2"]]
