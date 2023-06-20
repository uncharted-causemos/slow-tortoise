from prefect.storage import S3
from prefect.run_configs import DockerRun
from prefect.executors import LocalExecutor
from prefect import task, Flow
import os


"""
Simple prefect sanity check that runs in the local execution environment.
"""


@task(log_stdout=True)
def foo():
    print('Task run result: ')
    print('bar')
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

# TODO: based on the environment that the agent is running on, switch between DockerRun or KubeRun
flow.run_config = DockerRun(image=base_image)

# The flow code will be stored in and retrieved from following s3 bucket
flow.storage = S3(
    bucket='causemos-prod-prefect-flows-dev',
    stored_as_script=True,
)

if __name__ == "__main__":
    print('Task run result: ')
    state = flow.run()
    print(state.result[foo_result].result)
