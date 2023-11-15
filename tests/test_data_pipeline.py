import boto3
from moto import mock_s3

import pytest
import os.path
from prefect.utilities import debug
from utils import S3_DEST

from flows.data_pipeline import flow

##########################################A##############A##############
# Theses are smoke tests to run entire flow runs and make sure
# the pipeline is working without issues.
########################################################################


@pytest.fixture
def update_env(monkeypatch):
    # setup the environment overrides for the tests
    monkeypatch.setenv("WM_DASK_SCHEDULER", "")  # spawn local cluster
    monkeypatch.setenv("WM_DEST_TYPE", "s3")
    monkeypatch.setenv(
        "WM_S3_DEFAULT_INDICATOR_BUCKET", "test-indicators"
    )  # bucket name is used for file dir
    monkeypatch.setenv("WM_S3_DEFAULT_MODEL_BUCKET", "test-models")


@pytest.mark.skip(reason="Skip until unit tests are ready")
@mock_s3
def test_model(update_env):
    # connect to mock s3 storage
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket="test-indicators")

    try:
        from flows.data_pipeline import flow

        with debug.raise_on_exception():
            flow.run(
                parameters=dict(
                    model_id="geo-test-data",
                    run_id="test-run",
                    data_paths=["file://tests/data/geo-test-data.parquet"],
                )
            )
        assert True
    except:
        assert False


@pytest.mark.skip(reason="Skip until unit tests are ready")
@mock_s3
def test_indicator(update_env):
    # connect to mock s3 storage
    s3 = boto3.resource("s3")
    s3.create_bucket(Bucket="test-models")

    try:
        from flows.data_pipeline import flow

        with debug.raise_on_exception():
            flow.run(
                parameters=dict(
                    is_indicator=True,
                    model_id="ACLED",
                    run_id="indicator",
                    data_paths=["file://tests/data/acled-test.bin"],
                )
            )
        assert True
    except:
        assert False
