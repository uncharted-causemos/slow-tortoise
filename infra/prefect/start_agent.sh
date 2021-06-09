#!/bin/bash

prefect agent docker start \
    --label wm-prefect-server.openstack.uncharted.software \
    --api http://10.65.18.52:4200 \
    --network prefect-server \
    --env WM_DASK_SCHEDULER=10.65.18.83:8786 \
    --show-flow-logs
