#!/usr/bin/env python3
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../flows')))
import tiles_pb2

# Reads protobuf tile file and print the content. It is useful for inspecting and debugging a tile file.

def print_tile(tile):
  print(tile)

if len(sys.argv) != 2:
  print("Usage:", sys.argv[0], "TILE_FILE")
  sys.exit(-1)

tile = tiles_pb2.Tile()

# read
with open(sys.argv[1], "rb") as f:
  tile.ParseFromString(f.read())
  print_tile(tile)