#!/bin/bash

source ./config.sh

mkdir -p src

cp ../../requirements.txt src
cp -r ../../flows src

docker build -t $DOCKER_IMAGE:$DOCKER_IMAGE_VERSION .
