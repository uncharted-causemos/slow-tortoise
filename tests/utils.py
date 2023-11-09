import json
import pandas as pd
from io import StringIO
from datetime import datetime
from prefect import Flow
from prefect.executors import LocalExecutor
from pandas.testing import assert_frame_equal

S3_DEST = {"key": "key", "secret": "secret", "bucket": "test-bucket"}


# Returns wrapper function to execute a Prefect task with the arguments
def execute_prefect_task(task):
    def run_task(*args, **kwargs):
        with Flow("Task Execution Flow") as flow:
            result = task(*args, **kwargs)

        executor = LocalExecutor()
        flow_state = flow.run(executor=executor)

        if flow_state.is_successful():
            task_result = flow_state.result[result].result
            return task_result
        else:
            raise flow_state.result[result].result

    return run_task


# For given date string with format YYYY-mm-dd, return unix timestamp
def ts(date_string):
    return int(datetime.fromisoformat(date_string + "T00:00:00Z").timestamp()) * 1000


# Read s3 object as string
def read_obj(s3: object, path: str, bucket=S3_DEST["bucket"]) -> str:
    return s3.Object(bucket, path).get()["Body"].read().decode("utf-8")


# Compare equality of the two csv strings by converting them to pandas dataframe and comparing
def assert_csv_frame_equal(left_csv: str, right_csv: str):
    # Convert String into StringIO
    left = StringIO(left_csv.strip())
    right = StringIO(right_csv.strip())
    df_l = pd.read_csv(left, sep=",")
    df_r = pd.read_csv(right, sep=",")
    # assert equality
    assert_frame_equal(df_l, df_r)


# assert json equal ignoring the order of the top level keys
def assert_json_equal(left: str, right: str):
    assert json.loads(left) == json.loads(right)
