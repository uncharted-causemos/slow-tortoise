#!/bin/bash
prefect agent docker start --api http://10.65.18.83:4200/graphql --show-flow-logs --env WM_DASK_SCHEDULER=10.65.18.83:8786
