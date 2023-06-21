from prefect.storage import S3
from prefect.run_configs import DockerRun, LocalRun, KubernetesRun
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

WM_DATA_PIPELINE_IMAGE = os.getenv("WM_DATA_PIPELINE_IMAGE")
WM_FLOW_STORAGE_S3_BUCKET_NAME = os.getenv("WM_FLOW_STORAGE_S3_BUCKET_NAME")
WM_RUN_CONFIG_TYPE = os.getenv("WM_RUN_CONFIG_TYPE") # docker, local, kubernetes

with Flow("basic_flow") as flow:
    # The flow code will be stored in and retrieved from following s3 bucket
    # Note: aws s3 credentials must be available from `~/.aws/credentials` or from environment variables, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY 
    flow.storage = S3(
        bucket=WM_FLOW_STORAGE_S3_BUCKET_NAME,
        stored_as_script=True,
    )

    # Set flow run configuration. Each RunConfig type has a corresponding Prefect Agent.
    # Corresponding WM_RUN_CONFIG_TYPE environment variable must be provided by the agent with same type. 
    # For example, with docker agent, set RUN_CONFIG_TYPE to 'docker' and with kubernetes agent, set RUN_CONFIG_TYPE to 'kubernetes' 
    if WM_RUN_CONFIG_TYPE == 'docker':
        flow.run_config = DockerRun(image=WM_DATA_PIPELINE_IMAGE)
    elif WM_RUN_CONFIG_TYPE == 'local':
        flow.run_config = LocalRun()
    elif WM_RUN_CONFIG_TYPE == 'kubernetes':
        flow.run_config = KubernetesRun(image=WM_DATA_PIPELINE_IMAGE)

    flow.executor = LocalExecutor()

    foo_result = foo()

if __name__ == "__main__" and LOCAL_RUN:
    print('Task run result: ')
    state = flow.run()
    print(state.result[foo_result].result)
