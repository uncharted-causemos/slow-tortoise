import pytest
import os.path
from prefect.utilities import debug

@pytest.fixture
def update_env(monkeypatch):
    # setup the environment overrides for the tests
    monkeypatch.setenv("WM_LOCAL", "True")
    monkeypatch.setenv("WM_DASK_SCHEDULER", "") # spawn local cluster
    monkeypatch.setenv("WM_ELASTIC_URL", "")
    monkeypatch.setenv("WM_DEST_TYPE", "file") # skip writes
    monkeypatch.setenv("WM_S3_DEST_URL", "") # skip writes
    monkeypatch.setenv("WM_S3_DEFAULT_INDICATOR_BUCKET", "tests/output/test-indicators") # bucket name is used for file dir
    monkeypatch.setenv("WM_S3_DEFAULT_MODEL_BUCKET", "tests/output/test-models")

    if not os.path.exists('tests/output'):
        os.makedirs('tests/output')

def test_model(update_env):
    try:
        from flows.tile_v0 import flow
        with debug.raise_on_exception():
            flow.run(parameters=dict(compute_tiles=True, model_id='geo-test-data', run_id='test-run', data_paths=['file://tests/data/geo-test-data.parquet']))
        assert True
    except:
        assert False

def test_indicator(update_env):
    try:
        from flows.tile_v0 import flow
        with debug.raise_on_exception():
            flow.run(parameters=dict(is_indicator=True, model_id='ACLED', run_id='indicator', data_paths=['file://tests/data/acled-test.bin']))
        assert True
    except:
        assert False