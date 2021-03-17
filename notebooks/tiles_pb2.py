# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: tiles.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()




DESCRIPTOR = _descriptor.FileDescriptor(
  name='tiles.proto',
  package='tiles',
  syntax='proto3',
  serialized_options=b'Z+gitlab.uncharted.software/WM/wm-proto/tiles',
  create_key=_descriptor._internal_create_key,
  serialized_pb=b'\n\x0btiles.proto\x12\x05tiles\"4\n\tTileStats\x12\r\n\x05\x63ount\x18\x01 \x01(\x04\x12\x0b\n\x03sum\x18\x02 \x01(\x01\x12\x0b\n\x03\x61vg\x18\x03 \x01(\x01\"\x88\x01\n\x08TileBins\x12)\n\x05stats\x18\x01 \x03(\x0b\x32\x1a.tiles.TileBins.StatsEntry\x12\x11\n\ttotalBins\x18\x02 \x01(\r\x1a>\n\nStatsEntry\x12\x0b\n\x03key\x18\x01 \x01(\r\x12\x1f\n\x05value\x18\x02 \x01(\x0b\x32\x10.tiles.TileStats:\x02\x38\x01\"7\n\tTileCoord\x12\t\n\x01x\x18\x01 \x01(\r\x12\t\n\x01y\x18\x02 \x01(\r\x12\t\n\x01z\x18\x03 \x01(\r\x12\t\n\x01t\x18\x04 \x01(\x03\"F\n\x04Tile\x12\x1f\n\x05\x63oord\x18\x01 \x01(\x0b\x32\x10.tiles.TileCoord\x12\x1d\n\x04\x62ins\x18\x02 \x01(\x0b\x32\x0f.tiles.TileBinsB-Z+gitlab.uncharted.software/WM/wm-proto/tilesb\x06proto3'
)




_TILESTATS = _descriptor.Descriptor(
  name='TileStats',
  full_name='tiles.TileStats',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='count', full_name='tiles.TileStats.count', index=0,
      number=1, type=4, cpp_type=4, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='sum', full_name='tiles.TileStats.sum', index=1,
      number=2, type=1, cpp_type=5, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='avg', full_name='tiles.TileStats.avg', index=2,
      number=3, type=1, cpp_type=5, label=1,
      has_default_value=False, default_value=float(0),
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=22,
  serialized_end=74,
)


_TILEBINS_STATSENTRY = _descriptor.Descriptor(
  name='StatsEntry',
  full_name='tiles.TileBins.StatsEntry',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='key', full_name='tiles.TileBins.StatsEntry.key', index=0,
      number=1, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='value', full_name='tiles.TileBins.StatsEntry.value', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=b'8\001',
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=151,
  serialized_end=213,
)

_TILEBINS = _descriptor.Descriptor(
  name='TileBins',
  full_name='tiles.TileBins',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='stats', full_name='tiles.TileBins.stats', index=0,
      number=1, type=11, cpp_type=10, label=3,
      has_default_value=False, default_value=[],
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='totalBins', full_name='tiles.TileBins.totalBins', index=1,
      number=2, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[_TILEBINS_STATSENTRY, ],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=77,
  serialized_end=213,
)


_TILECOORD = _descriptor.Descriptor(
  name='TileCoord',
  full_name='tiles.TileCoord',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='x', full_name='tiles.TileCoord.x', index=0,
      number=1, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='y', full_name='tiles.TileCoord.y', index=1,
      number=2, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='z', full_name='tiles.TileCoord.z', index=2,
      number=3, type=13, cpp_type=3, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='t', full_name='tiles.TileCoord.t', index=3,
      number=4, type=3, cpp_type=2, label=1,
      has_default_value=False, default_value=0,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=215,
  serialized_end=270,
)


_TILE = _descriptor.Descriptor(
  name='Tile',
  full_name='tiles.Tile',
  filename=None,
  file=DESCRIPTOR,
  containing_type=None,
  create_key=_descriptor._internal_create_key,
  fields=[
    _descriptor.FieldDescriptor(
      name='coord', full_name='tiles.Tile.coord', index=0,
      number=1, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
    _descriptor.FieldDescriptor(
      name='bins', full_name='tiles.Tile.bins', index=1,
      number=2, type=11, cpp_type=10, label=1,
      has_default_value=False, default_value=None,
      message_type=None, enum_type=None, containing_type=None,
      is_extension=False, extension_scope=None,
      serialized_options=None, file=DESCRIPTOR,  create_key=_descriptor._internal_create_key),
  ],
  extensions=[
  ],
  nested_types=[],
  enum_types=[
  ],
  serialized_options=None,
  is_extendable=False,
  syntax='proto3',
  extension_ranges=[],
  oneofs=[
  ],
  serialized_start=272,
  serialized_end=342,
)

_TILEBINS_STATSENTRY.fields_by_name['value'].message_type = _TILESTATS
_TILEBINS_STATSENTRY.containing_type = _TILEBINS
_TILEBINS.fields_by_name['stats'].message_type = _TILEBINS_STATSENTRY
_TILE.fields_by_name['coord'].message_type = _TILECOORD
_TILE.fields_by_name['bins'].message_type = _TILEBINS
DESCRIPTOR.message_types_by_name['TileStats'] = _TILESTATS
DESCRIPTOR.message_types_by_name['TileBins'] = _TILEBINS
DESCRIPTOR.message_types_by_name['TileCoord'] = _TILECOORD
DESCRIPTOR.message_types_by_name['Tile'] = _TILE
_sym_db.RegisterFileDescriptor(DESCRIPTOR)

TileStats = _reflection.GeneratedProtocolMessageType('TileStats', (_message.Message,), {
  'DESCRIPTOR' : _TILESTATS,
  '__module__' : 'tiles_pb2'
  # @@protoc_insertion_point(class_scope:tiles.TileStats)
  })
_sym_db.RegisterMessage(TileStats)

TileBins = _reflection.GeneratedProtocolMessageType('TileBins', (_message.Message,), {

  'StatsEntry' : _reflection.GeneratedProtocolMessageType('StatsEntry', (_message.Message,), {
    'DESCRIPTOR' : _TILEBINS_STATSENTRY,
    '__module__' : 'tiles_pb2'
    # @@protoc_insertion_point(class_scope:tiles.TileBins.StatsEntry)
    })
  ,
  'DESCRIPTOR' : _TILEBINS,
  '__module__' : 'tiles_pb2'
  # @@protoc_insertion_point(class_scope:tiles.TileBins)
  })
_sym_db.RegisterMessage(TileBins)
_sym_db.RegisterMessage(TileBins.StatsEntry)

TileCoord = _reflection.GeneratedProtocolMessageType('TileCoord', (_message.Message,), {
  'DESCRIPTOR' : _TILECOORD,
  '__module__' : 'tiles_pb2'
  # @@protoc_insertion_point(class_scope:tiles.TileCoord)
  })
_sym_db.RegisterMessage(TileCoord)

Tile = _reflection.GeneratedProtocolMessageType('Tile', (_message.Message,), {
  'DESCRIPTOR' : _TILE,
  '__module__' : 'tiles_pb2'
  # @@protoc_insertion_point(class_scope:tiles.Tile)
  })
_sym_db.RegisterMessage(Tile)


DESCRIPTOR._options = None
_TILEBINS_STATSENTRY._options = None
# @@protoc_insertion_point(module_scope)