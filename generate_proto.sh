#!/bin/sh

# Installing protoc: 'brew install protobuf'
# Make sure you have wm-proto repo in your parent directory
protoc --proto_path=../wm-proto/proto/ --python_out=./flows/ tiles.proto