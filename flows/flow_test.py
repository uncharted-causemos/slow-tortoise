from prefect.storage import Docker
from prefect.executors import LocalExecutor
from prefect import task, Flow
import os


"""
Simple prefect sanity check that runs in the local execution environment.
"""

@task
def foo():
    return "bar"

LOCAL_RUN = os.getenv("WM_LOCAL", "False").lower() in ("true", "1", "t")
PUSH_IMAGE = os.getenv("WM_PUSH_IMAGE", "False").lower() in ("true", "1", "t")

with Flow("basic_flow") as flow:
    foo_result = foo()

executor = LocalExecutor()
flow.executor = executor

flow.storage = Docker(
    registry_url= "docker.uncharted.software" if PUSH_IMAGE else "",
    base_image=f"docker.uncharted.software/worldmodeler/wm-data-pipeline:latest",
    local_image=True,
    stored_as_script=True,
    path="/wm_data_pipeline/flows/flow_test.py",
)

if __name__ == "__main__" and LOCAL_RUN:
    state = flow.run()
    print(state.result[foo_result].result)