from prefect import Flow
from prefect.executors import LocalExecutor

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