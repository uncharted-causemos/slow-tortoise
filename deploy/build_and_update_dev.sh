#!/bin/bash
set -e

# THIS SCRIPT REQUIRES THE FOLLOWING ENTRIES IN ~/.ssh/config
# Host dask-swarm-test                                                             
#     HostName 10.65.18.107                                                        
#     User centos


source ./test.env

SCRIPT_DIR="$(dirname "$0")"
pushd $SCRIPT_DIR

echo "Stopping swarms..."
ssh dask-swarm-test 'docker stack rm dask_swarm'

echo "Building docker..."
pushd ../infra/docker
./docker_build.sh
./docker_push.sh
popd

echo "Pulling images to swarms..."
ssh dask-swarm-test "docker pull $WM_DATA_PIPELINE_IMAGE"

echo "Restarting swarms..."
ssh dask-swarm-test 'docker stack deploy --with-registry-auth --compose-file docker-dask-docker-compose.yml dask_swarm'

echo "Registering with Prefect..."
./register_flows.sh

popd
echo "SUCCESS!"
