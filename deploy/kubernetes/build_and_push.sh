#!/bin/bash
if [ "$1" == "-p" ]; then
  source ./prod.env
elif [ "$1" == "-s" ]; then
  source ./staging.env
else
  source ./local.env
fi
echo "Building docker image $WM_DATA_PIPELINE_IMAGE..."

pushd ../../infra/docker
./docker_build.sh
./docker_push.sh
popd
