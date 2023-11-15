import pandas as pd
import dask.dataframe as dd

import boto3
from moto import mock_s3

from ..utils import execute_prefect_task, read_obj, assert_json_equal, S3_DEST
from flows.data_pipeline import record_qualifier_lists, DEFAULT_PARTITIONS

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
    "qual2": ["q2a", "q2a", "q2b"],
}


@mock_s3
def test_record_qualifier_lists():
    # connect to mock s3 storage
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket=S3_DEST["bucket"])

    df = dd.from_pandas(pd.DataFrame(data, index=[0, 1, 2]), npartitions=DEFAULT_PARTITIONS)

    qualifier_counts = execute_prefect_task(record_qualifier_lists)(
        df, S3_DEST, "mid-1", "rid-1", [["qual1"], ["qual2"]], 10
    )

    assert {"A": {"qual1": 2, "qual2": 1}, "B": {"qual1": 1, "qual2": 1}} == qualifier_counts

    assert_json_equal('["d", "e"]', read_obj(s3, "mid-1/rid-1/raw/A/info/qualifiers/qual1.json"))
    assert_json_equal('["f"]', read_obj(s3, "mid-1/rid-1/raw/B/info/qualifiers/qual1.json"))
    assert_json_equal('["q2a"]', read_obj(s3, "mid-1/rid-1/raw/A/info/qualifiers/qual2.json"))
    assert_json_equal('["q2b"]', read_obj(s3, "mid-1/rid-1/raw/B/info/qualifiers/qual2.json"))
    assert_json_equal(
        '{"thresholds": 10, "counts": {"qual1": 2, "qual2": 1}}',
        read_obj(s3, "mid-1/rid-1/raw/A/info/qualifier_counts.json"),
    )
    assert_json_equal(
        '{"thresholds": 10, "counts": {"qual1": 1, "qual2": 1}}',
        read_obj(s3, "mid-1/rid-1/raw/B/info/qualifier_counts.json"),
    )
