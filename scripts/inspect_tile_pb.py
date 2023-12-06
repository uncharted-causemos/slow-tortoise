#!/usr/bin/env python3
import sys
from typing import cast, Any

# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../flows")))
from flows import tiles_pb2

# Reads a protobuf tile file and print the content. It is useful for inspecting and debugging a tile file.
# Example: ./inspect_tile_pb 1546300800000-5-19-15.tile


def print_tile(tile):
    print(tile)


if len(sys.argv) != 2:
    print("Usage:", sys.argv[0], "TILE_FILE")
    print("Example:", sys.argv[0], "1546300800000-5-19-15.tile")
    sys.exit(-1)

tile = cast(Any, tiles_pb2).Tile()

# read
with open(sys.argv[1], "rb") as f:
    tile.ParseFromString(f.read())
    print_tile(tile)
