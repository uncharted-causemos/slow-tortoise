from datetime import datetime
from prefect import Flow
from prefect.executors import LocalExecutor

S3_DEST = {
    "key": "key",
    "secret": "secret",
    "bucket": "test-bucket"
}

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
