#!/bin/sh

# Make sure you have wm-proto repo in your parent directory
protoc --proto_path=../wm-proto/proto/ --python_out=. tiles.proto
