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
    print("Task run result: ")
    print("bar")
    return "bar"


LOCAL_RUN = os.getenv("WM_LOCAL", "False").lower() in ("true", "1", "t")

WM_DATA_PIPELINE_IMAGE = os.getenv("WM_DATA_PIPELINE_IMAGE", "")
WM_FLOW_STORAGE_S3_BUCKET_NAME = os.getenv("WM_FLOW_STORAGE_S3_BUCKET_NAME", "")
WM_RUN_CONFIG_TYPE = os.getenv("WM_RUN_CONFIG_TYPE", "")  # docker, local, kubernetes

# Custom s3 destination. If WM_S3_DEST_URL is not empty, the pipeline will use following information to connect s3 to write output to,
# otherwise it will use default aws s3 with above AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
# If you want to write the pipeline output to custom location such as custom minio storage, provide following information
WM_S3_DEST_URL = os.getenv("WM_S3_DEST_URL", None)
WM_S3_DEST_KEY = os.getenv("WM_S3_DEST_KEY")
WM_S3_DEST_SECRET = os.getenv("WM_S3_DEST_SECRET")
WM_S3_DEST_REGION = "us-east-1"

with Flow("basic_flow") as flow:
    # The flow code will be stored in and retrieved from following s3 bucket
    # Note: aws s3 credentials must be available from `~/.aws/credentials` or from environment variables, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY
    flow.storage = S3(
        bucket=WM_FLOW_STORAGE_S3_BUCKET_NAME,
        stored_as_script=True,
        client_options=None  # type: ignore
        if not WM_S3_DEST_URL
        else {
            "endpoint_url": WM_S3_DEST_URL,
            "region_name": WM_S3_DEST_REGION,
            "aws_access_key_id": WM_S3_DEST_KEY,
            "aws_secret_access_key": WM_S3_DEST_SECRET,
        },
    )

    # Set flow run configuration. Each RunConfig type has a corresponding Prefect Agent.
    # Corresponding WM_RUN_CONFIG_TYPE environment variable must be provided by the agent with same type.
    # For example, with docker agent, set RUN_CONFIG_TYPE to 'docker' and with kubernetes agent, set RUN_CONFIG_TYPE to 'kubernetes'
    if WM_RUN_CONFIG_TYPE == "docker":
        flow.run_config = DockerRun(image=WM_DATA_PIPELINE_IMAGE)
    elif WM_RUN_CONFIG_TYPE == "local":
        flow.run_config = LocalRun()
    elif WM_RUN_CONFIG_TYPE == "kubernetes":
        flow.run_config = KubernetesRun(image=WM_DATA_PIPELINE_IMAGE)

    flow.executor = LocalExecutor()

    foo_result = foo()

if __name__ == "__main__" and LOCAL_RUN:
    print("Task run result: ")
    state = flow.run()
    if state:
        print(state.result[foo_result].result)
