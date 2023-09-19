#!/bin/bash
source ./prod.env
echo "Building docker..."

pushd ../../infra/docker
./docker_build.sh
./docker_push.sh
popd
