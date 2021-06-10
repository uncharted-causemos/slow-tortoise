#!/bin/bash

source ./config.sh

mkdir -p src

cp ../../setup.py src
cp ../../version.py src
cp -r ../../flows src

docker build -t $DOCKER_IMAGE:$DOCKER_IMAGE_VERSION .
