#!/bin/bash

source ./config.sh

echo "\nCopying flow source code\n"
rm -rf src
mkdir -p src

cp ../../setup.py src
cp ../../version.py src
cp -r ../../flows src

echo "\nRunning docker build\n"
docker build --no-cache -t $DOCKER_IMAGE:$DOCKER_IMAGE_VERSION .
