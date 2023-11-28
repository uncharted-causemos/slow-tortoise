import boto3
from moto import mock_s3

import pytest
from .utils import read_obj

from flows.data_pipeline import flow

##########################################A##############A##############
# Theses are smoke tests to run entire flow runs and make sure
# the pipeline is working without issues.
########################################################################


@pytest.fixture
def update_env(monkeypatch):
    # setup the environment overrides for the tests
    monkeypatch.setenv("WM_DASK_SCHEDULER", None)
    monkeypatch.setenv("WM_DEST_TYPE", "s3")


@mock_s3
def test_model(update_env):
    bucket = "models-test"
    # connect to mock s3 storage
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket=bucket)

    flow.run(
        parameters=dict(
            model_id="geo-test-data",
            run_id="test-run",
            data_paths=["file://tests/data/geo-test-data.parquet"],
            model_bucket=bucket,
        )
    )

    assert isinstance(
        read_obj(
            s3,
            "geo-test-data/test-run/month/feature1/regional/country/timeseries/default/Ethiopia.csv",
            bucket=bucket,
        ),
        str,
    )


@mock_s3
def test_indicator(update_env):
    bucket = "indicators-test"
    # connect to mock s3 storage
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket=bucket)

    flow.run(
        parameters=dict(
            is_indicator=True,
            model_id="ACLED",
            run_id="indicator",
            data_paths=["file://tests/data/acled-test.bin"],
            indicator_bucket=bucket,
        )
    )

    assert isinstance(
        read_obj(
            s3,
            "ACLED/indicator/year/fatalities/regional/admin1/timeseries/default/Ethiopia__Gambela Region.csv",
            bucket=bucket,
        ),
        str,
    )
