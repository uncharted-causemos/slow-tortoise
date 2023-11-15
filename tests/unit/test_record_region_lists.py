import pandas as pd
import dask.dataframe as dd

import boto3
from moto import mock_s3

from ..utils import execute_prefect_task, read_obj, assert_json_equal, S3_DEST
from flows.data_pipeline import record_region_lists, DEFAULT_PARTITIONS

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
    "qual1": ["d", "e", "f"],
    "qual2": ["q2a", "q2b", "q2c"],
}


@mock_s3
def test_record_region_lists():
    # connect to mock s3 storage
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket=S3_DEST["bucket"])

    df = dd.from_pandas(pd.DataFrame(data, index=[0, 1, 2]), npartitions=DEFAULT_PARTITIONS)

    region_cols, feature_list = execute_prefect_task(record_region_lists)(
        df, S3_DEST, "mid-1", "rid-1"
    )

    assert_json_equal(
        '{"country": ["United States", "Canada"], "admin1": ["United States__New York", "Canada__Ontario"], "admin2": ["United States__New York__nyadmi2", "Canada__Ontario__onadmin2"], "admin3": ["United States__New York__nyadmi2__nyadmin3", "Canada__Ontario__onadmin2__onadmin3"]}',
        read_obj(s3, "mid-1/rid-1/raw/A/info/region_lists.json"),
    )

    assert_json_equal(
        '{"country": ["Canada"], "admin1": ["Canada__Quebec"], "admin2": ["Canada__Quebec__qadmin2"], "admin3": ["Canada__Quebec__qadmin2__qadmin3"]}',
        read_obj(s3, "mid-1/rid-1/raw/B/info/region_lists.json"),
    )

    assert ["country", "admin1", "admin2", "admin3"] == region_cols
    assert ["A", "B"] == feature_list
