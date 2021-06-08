#!/bin/bash
mkdir -p src

cp ../../requirements.txt src
cp -r ../../flows src

docker build -t docker.uncharted.software/worldmodeler/wm-data-pipeline:latest .
