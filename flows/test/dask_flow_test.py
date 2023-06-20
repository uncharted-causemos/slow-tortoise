from prefect import task, Flow
from prefect.storage import S3
from prefect.run_configs import DockerRun
from prefect.executors import DaskExecutor, LocalDaskExecutor
import dask
import os
import time
import random


@task(log_stdout=True)
def simple_add():
    def inc(x):
        time.sleep(random.random())
        return x + 1

    def dec(x):
        time.sleep(random.random())
        return x - 1

    def add(x, y):
        time.sleep(random.random())
        return x + y

    inc = dask.delayed(inc)
    dec = dask.delayed(dec)
    add = dask.delayed(add)

    x = inc(1)
    y = dec(2)
    z = add(x, y)
    result = z.compute()
    print("result", result)

    return result


DASK_SCHEDULER = os.getenv("WM_DASK_SCHEDULER")
LOCAL_RUN = os.getenv("WM_LOCAL", "False").lower() in ("true", "1", "t")

# DO NOT DECLARE FLOW IN MAIN.  During registration, prefect calls `exec` on this
# script and looks for instances of `Flow` at the global level.
with Flow("dask_flow") as flow:
    simple_add_result = simple_add()

if not DASK_SCHEDULER:
    flow.executor = LocalDaskExecutor()
else:
    flow.executor = DaskExecutor(DASK_SCHEDULER)

# The flow will be executed inside this docker image
base_image = os.getenv(
    "WM_DATA_PIPELINE_IMAGE", "docker.uncharted.software/worldmodeler/wm-data-pipeline:latest"
)
# TODO: based on the environment that the agent is running on, switch between DockerRun or KubeRun
flow.run_config = DockerRun(image=base_image)

# The flow code will be stored in and retrieved from following s3 bucket
# 
flow.storage = S3(
    bucket='causemos-prod-prefect-flows-dev',
    stored_as_script=True,
)

# For debugging support - local dask cluster needs to run in main otherwise process forking
# fails.
if __name__ == "__main__" and LOCAL_RUN:
    print('test')
    state = flow.run()
    print(state.result[simple_add_result].result)
