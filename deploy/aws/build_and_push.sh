#!/bin/bash
source ./prod.env
echo "Building docker..."

../../infra/docker/docker_build.sh
../../infra/docker/docker_push.sh