#!/bin/bash
if [ "$1" == "-p" ]; then
  source ./prod.env
else
  source ./staging.env
fi
echo "Building docker image $WM_DATA_PIPELINE_IMAGE..."

pushd ../../infra/docker
./docker_build.sh
./docker_push.sh
popd
