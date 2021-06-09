#!/bin/bash
prefect agent docker start --api http://10.65.18.52:4200 --env WM_DASK_SCHEDULER=10.65.18.83:8786
