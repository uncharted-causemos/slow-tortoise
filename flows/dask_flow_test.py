from prefect import task, Flow
from prefect.storage import Docker
from prefect.executors import DaskExecutor
import dask
import os
import time
import random

@task
def foo():
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

    return result

DASK_SCHEDULER = os.getenv("WM_DASK_SCHEDULER")
LOCAL_RUN = os.getenv("WM_LOCAL", "False").lower() in ("true", "1", "t")
PUSH_IMAGE = os.getenv("WM_PUSH_IMAGE", "False").lower() in ("true", "1", "t")

# DO NOT DECLARE FLOW IN MAIN.  During registration, prefect calls `exec` on this
# script and looks for instances of `Flow` at the global level.
with Flow("dask_flow") as flow:
    foo_result = foo()

executor = DaskExecutor() if DASK_SCHEDULER == "" else DaskExecutor(DASK_SCHEDULER)
flow.executor = executor
flow.storage = Docker(
    registry_url= "docker.uncharted.software" if PUSH_IMAGE else "",
    base_image=f"docker.uncharted.software/worldmodeler/wm-data-pipeline:latest",
    local_image=True,
    stored_as_script=True,
    path="/wm_data_pipeline/flows/dask_flow_test.py",
    ignore_healthchecks=True,
)

# For debugging support - local dask cluster needs to run in main otherwise process forking
# fails.
if __name__ == "__main__" and LOCAL_RUN:
    state = flow.run(executor=executor)
    print(state.result[foo_result].result)