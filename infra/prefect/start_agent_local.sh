#!/bin/bash
prefect agent docker start --api http://172.20.0.6:4200 --network prefect-server --show-flow-logs --env WM_DASK_SCHEDULER=scheduler:8786
