#!/bin/bash
APOLLO_ADDR=$(docker inspect -f '{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}' tmp_apollo_1 )

if [ -z ${APOLLO_ADDR+x} ]; then echo "ERROR: Can't find prefect API address: start server before launching agent"; exit 1; fi

prefect agent docker start --api http://$APOLLO_ADDR:4200 --label wm-prefect-server.openstack.uncharted.software  --network prefect-server --show-flow-logs --env WM_DASK_SCHEDULER=scheduler:8786
