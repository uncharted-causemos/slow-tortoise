#!/bin/bash
set -e

# THIS SCRIPT REQUIRES THE FOLLOWING ENTRIES IN ~/.ssh/config
# Host dask-swarm
#     HostName 10.65.18.58
#     User centos

# Host dask-swarm-big
#     HostName 10.65.18.82
#     User centos

source ./dev.env

SCRIPT_DIR="$(dirname "$0")"
pushd $SCRIPT_DIR

echo "Stopping request-queue"
curl -X PUT $WM_QUEUE_MANAGER_CAUSEMOS/data-pipeline/stop
curl -X PUT $WM_QUEUE_MANAGER_ANALYST/data-pipeline/stop
curl -X PUT $WM_QUEUE_MANAGER_MODELER/data-pipeline/stop

echo "Stopping swarms..."
ssh dask-swarm 'docker stack rm dask_swarm'
ssh dask-swarm-big 'docker stack rm big_dask_swarm'

echo "Building docker..."
pushd ../../infra/docker
./docker_build.sh
./docker_push.sh
popd

echo "Pulling images to swarms..."
ssh dask-swarm "docker pull $WM_DATA_PIPELINE_IMAGE"
ssh dask-swarm-big "docker pull $WM_DATA_PIPELINE_IMAGE"

echo "Restarting swarms..."
ssh dask-swarm 'docker stack deploy --with-registry-auth --compose-file docker-compose.yml dask_swarm'
ssh dask-swarm-big 'docker stack deploy --with-registry-auth --compose-file docker-compose.yml big_dask_swarm'

echo "Registering with Prefect..."
./register_flows.sh

echo "Starting request-queue"
curl -X PUT $WM_QUEUE_MANAGER_CAUSEMOS/data-pipeline/start
curl -X PUT $WM_QUEUE_MANAGER_ANALYST/data-pipeline/start
curl -X PUT $WM_QUEUE_MANAGER_MODELER/data-pipeline/start

popd
echo "SUCCESS!"

