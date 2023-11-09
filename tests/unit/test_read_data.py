import pytest
import pandas as pd
import dask.dataframe as dd
from unittest.mock import patch
from prefect.engine.signals import FAIL

from ..utils import execute_prefect_task
from flows.data_pipeline import read_data


# Mocked DataFrame for testing
def create_mock_dataframe(path, engine):
    # Mock read_data and returns df based on provided path params
    if path == "dummy.parquet":
        data = {
            "timestamp": [1, 2, 3],
            "country": ["United States", "Canada", "Canada"],
            "admin1": ["New York", "Ontario", "Quebec"],
            "admin2": ["nyadmi2", "onadmin2", "qadmin2"],
            "admin3": ["nyadmin3", "onadmin3", "qadmin3"],
            "lat": [2.1, 1.2, 9.2],
            "lng": [1.3, 1.2, 8.1],
            "feature": ["A", "B", "B"],
            "value": [4.2, 5.1, 1.2],
            "qual1": ["a", "b", "c"],
        }
        df = pd.DataFrame(data, index=[0, 1, 2])
        return df
    if path == "dummy2.parquet":
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
        df = pd.DataFrame(data, index=[0, 1, 2])
        return df


@pytest.fixture
def mock_read_parquet():
    with patch("pandas.read_parquet", side_effect=create_mock_dataframe):
        yield


def test_read_data_single_parquet_file(mock_read_parquet):
    ddf, num_rows = execute_prefect_task(read_data)({}, ["dummy.parquet"])

    assert isinstance(ddf, dd.DataFrame)
    assert num_rows == 3

    df = ddf.compute()
    assert df.loc[1, "country"] == "Canada"
    assert df.loc[2, "value"] == 1.2


def test_read_data_multiple_parquet_files(mock_read_parquet):
    ddf, num_rows = execute_prefect_task(read_data)({}, ["dummy.parquet", "dummy2.parquet"])

    assert isinstance(ddf, dd.DataFrame)
    assert num_rows == 6

    df = ddf.compute().sort_values(by=["timestamp"], ignore_index=True)
    assert df.loc[1, "country"] == "Canada"
    assert df.loc[1, "qual2"] == ""
    assert df.loc[5, "qual2"] == "q2c"


def test_read_data_fail_with_no_parquet_files(mock_read_parquet):
    with pytest.raises(FAIL) as exc_info:
        execute_prefect_task(read_data)({}, [])

    assert exc_info.type == FAIL
    assert str(exc_info.value) == "No parquet files provided"


def test_read_data_fail_with_no_numeric_parquet_files(mock_read_parquet):
    with pytest.raises(FAIL) as exc_info:
        execute_prefect_task(read_data)(
            {},
            [
                "data.dummy_str.parquet.gzip",
                "data.dummy_str.0.parquet.gzip",
                "data.dummy_str.1.parquet.gzip",
            ],
        )

    assert exc_info.type == FAIL
    assert str(exc_info.value) == "No numeric parquet files"
