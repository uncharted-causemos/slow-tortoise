
import pandas as pd
import dask.dataframe as dd
import numpy as np

from ..utils import execute_prefect_task
from flows.data_pipeline import validate_and_fix, DEFAULT_PARTITIONS, MAX_TIMESTAMP

def test_validate_and_fix_drop_cols():
    data = {
        "timestamp": [1, 2, 3],
        "feature": ["A", "B", "C"],
        "value": [1.1, 2.2, 3.3],
        "lat": [1.1, 2.2, None],
        "lng": [None, 2.2, 3.3],
        "country": [None, None, None],
        "other_col": [1, None, None],
        "other_col2": [None, None, None]
    }
    df = dd.from_pandas(pd.DataFrame(data), npartitions=DEFAULT_PARTITIONS)
    result, _, _, _, _ = execute_prefect_task(validate_and_fix)(df, "weight", 0)

    expected_columns = ["timestamp", "feature", "value", "lat", "lng", "other_col"]
    assert set(result.columns) == set(expected_columns)

def test_validate_and_fix_ensure_dtype():
    data = {
        "timestamp": [1, 2, 3],
        "feature": ["A", "B", "C"],
        "value": ["1.1", "2.2", "3.3"],
        "lat": ["string", 2.2, "3.3"],
        "lng": ["1.1", 2.2, "3.3"],
    }
    df = dd.from_pandas(pd.DataFrame(data), npartitions=DEFAULT_PARTITIONS)
    result, _, _, _, _ = execute_prefect_task(validate_and_fix)(df, "weight", 0)

    assert result["value"].dtype == "float64"
    assert result["lat"].dtype == "float64"
    assert result["lng"].dtype == "float64"
    assert pd.isna(result.compute().loc[0, 'lat'])

def test_validate_and_fix_handle_missing_vals():
    data = {
        "timestamp": [1, None, 3],
        "feature": ["A", None, "C"],
        "value": [None, None, None],
        "lat": [1.1, 2.2, 3.3],
        "lng": [1.1, 2.2, 3.3],
        "country": ["val1", None, "val2"],
    }
    df = dd.from_pandas(pd.DataFrame(data), npartitions=DEFAULT_PARTITIONS)
    result, _, num_missing_ts, _, num_missing_val = execute_prefect_task(validate_and_fix)(df, "weight", 0)

    r = result.compute()
    assert num_missing_ts == 1  # One missing timestamp
    assert num_missing_val == 3  # All 'value' columns are missing
    assert r["timestamp"].tolist() == [1, 0, 3]
    assert pd.isna(r["value"].tolist()[0])
    assert r["country"].tolist() == ["val1", "None", "val2"] # Fill None with 'None'

def test_validate_and_fix_handle_weight_col():
    data = {
        "timestamp": [1, 2, 3],
        "feature": ["A", "B", "C"],
        "value": [1.1, 2.2, 3.3],
        "lat": [1.1, 2.2, 3.3],
        "lng": [1.1, 2.2, 3.3],
        "country": [None, None, None],
        "other_col": ["1", None, 2],
    }
    df = dd.from_pandas(pd.DataFrame(data), npartitions=DEFAULT_PARTITIONS)
    result, weight_col, _, _, _ = execute_prefect_task(validate_and_fix)(df, "other_col", 0)

    r = result.compute()
    # Test handling missing vals and types
    assert weight_col == "other_col"
    assert r["other_col"].dtype == 'float64'
    assert r["other_col"].tolist() == [1, 0, 2]

    _, weight_col, _, _, _ = execute_prefect_task(validate_and_fix)(df, "other_col2", 0)
    assert weight_col == ""

    _, weight_col, _, _, _ = execute_prefect_task(validate_and_fix)(df, "", 0)
    assert weight_col == ""

def test_validate_and_fix_handle_invalid_vals():
    data = {
        "timestamp": [1, 2, MAX_TIMESTAMP + 1],
        "feature": ["A", "B", "C"],
        "value": [np.inf, -np.inf, 3.4],
        "lat": [1.1, 2.2, 3.3],
        "lng": [1.1, 2.2, 3.3],
        "country": ["c1", "c2", "c3"],
        "admin1": ["val1//2", "val2", "val3"],
    }
    df = dd.from_pandas(pd.DataFrame(data), npartitions=DEFAULT_PARTITIONS)
    result, _, _, num_invalid_ts, _ = execute_prefect_task(validate_and_fix)(df, "weight", 0)

    r = result.compute()
    assert num_invalid_ts == 1
    assert len(r.index) == 2 # rows with invalid ts will be removed
    assert r["timestamp"].tolist() == [1, 2]

    assert pd.isna(r["value"].tolist()[0]) and pd.isna(r["value"].tolist()[1]) # remove infinities

    # handle characters that minio can't handle
    assert r["admin1"].tolist() == ["val12", "val2"]


    # assert r["admin1"].tolist() == ["val1", "None", "val2"] # Fill None with 'None'

# def test_validate_and_fix_invalid_timestamp():
#     data = {
#         "timestamp": [1, 2, MAX_TIMESTAMP + 1],
#         "feature": ["A", "B", "C"],
#         "value": [1.1, 2.2, 3.3],
#     }
#     df = dd.from_pandas(pd.DataFrame(data))
#     _, _, _, num_invalid_ts, _ = validate_and_fix(df, "weight", 0)

#     assert num_invalid_ts == 1  # One invalid timestamp