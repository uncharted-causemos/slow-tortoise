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
def read_obj(s3, path: str, bucket=S3_DEST["bucket"]) -> str:
    return s3.Object(bucket, path).get()["Body"].read().decode("utf-8")


# Read protobuf from s3 and parse
def read_proto(s3, path: str, proto_obj, bucket=S3_DEST["bucket"]) -> str:
    obj = s3.Object(bucket, path).get()["Body"].read()
    proto_obj.ParseFromString(obj)
    return proto_obj


# Compare equality of the two csv strings by converting them to pandas dataframe and comparing
def assert_csv_frame_equal(left_csv: str, right_csv: str, sort=True, sort_by=None, strip_line=True):
    if strip_line:
        left_csv = "\n".join([str.strip() for str in left_csv.splitlines()])
        right_csv = "\n".join([str.strip() for str in right_csv.splitlines()])
    # Convert String into StringIO
    left = StringIO(left_csv)
    right = StringIO(right_csv)
    df_l = pd.read_csv(left, sep=",")
    df_r = pd.read_csv(right, sep=",")

    if sort and not sort_by:
        sort_by = [df_l.columns[0], df_l.columns[1]]

    df_l = df_l.sort_values(by=sort_by, ignore_index=True)
    df_r = df_r.sort_values(by=sort_by, ignore_index=True)
    # assert equality
    try:
        assert_frame_equal(df_l, df_r)
    except AssertionError as e:
        # Extend the error message and re throw
        msg = f"\n\nleft_csv:\n{left_csv}\nright_csv:\n{right_csv}\n"
        raise AssertionError(f"{e}{msg}")


# Compare equality of the two proto buf objects
def assert_proto_equal(left_proto: object, right_proto: object):
    try:
        assert left_proto == right_proto
    except AssertionError as e:
        # Extend the error message and re throw
        msg = f"\n\nleft_proto:\n{left_proto}\nright_proto:\n{right_proto}\n"
        raise AssertionError(f"{e}{msg}")


# assert json equal ignoring the order of the top level keys
def assert_json_equal(left: str, right: str, sort_list=False):
    try:
        l_d = json.loads(left)
        r_d = json.loads(right)
        if sort_list:
            l_d = sort_lists_in_json_obj(l_d)
            r_d = sort_lists_in_json_obj(r_d)
        assert l_d == r_d
    except AssertionError as e:
        # Extend the error message and re throw
        msg = f"\n\nleft:\n{left}\nright:\n{right}\n"
        raise AssertionError(f"{e}{msg}")


def sort_lists_in_json_obj(json_obj):
    if isinstance(json_obj, list):
        return sorted(json_obj)
    elif isinstance(json_obj, dict):
        for key, value in json_obj.items():
            # Recursively sort nested dictionaries
            json_obj[key] = sort_lists_in_json_obj(value)
        return json_obj
    else:
        return json_obj
