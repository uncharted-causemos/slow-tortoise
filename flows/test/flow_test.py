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

base_image = os.getenv(
    "WM_DATA_PIPELINE_IMAGE", "docker.uncharted.software/worldmodeler/wm-data-pipeline:latest"
)
registry_url = os.getenv("DOCKER_REGISTRY_URL", "docker.uncharted.software")
image_name = os.getenv("DOCKER_RUN_IMAGE", "worldmodeler/wm-data-pipeline/test-flow")
if not PUSH_IMAGE:
    image_name = f"{registry_url}/{image_name}"
    registry_url = None

flow.storage = Docker(
    registry_url=registry_url,
    base_image=base_image,
    image_name=image_name,
    local_image=True,
    stored_as_script=True,
    path="/wm_data_pipeline/flows/test/flow_test.py",
    ignore_healthchecks=True,
)

if __name__ == "__main__" and LOCAL_RUN:
    state = flow.run()
    print(state.result[foo_result].result)
