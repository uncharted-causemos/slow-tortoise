from prefect import task, Flow
from prefect.storage import Docker
from prefect.executors import DaskExecutor, LocalDaskExecutor
import dask
import os
import time
import random

@task
def foo():
    def inc(x):
        time.sleep(random.random()*5)
        return x + 1


    def dec(x):
        time.sleep(random.random()*5)
        return x - 1


    def add(x, y):
        time.sleep(random.random()*5)
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

with Flow("throughput_test") as flow:
    for i in range(0, 500):
        foo()

    flow.executor = DaskExecutor(DASK_SCHEDULER)

# For debugging support - local dask cluster needs to run in main otherwise process forking
# fails.
if __name__ == "__main__" and LOCAL_RUN:
    flow.run()