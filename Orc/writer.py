import json
import os
from xml.etree import ElementTree as ET

import pyorc

SOURCE_FILE = 'data/paris/2.250182,48.818215,2.251182,48.819215.osm'
ORC_FILE = 'Orc/output/nodes.orc'
ORC_SNAPPY_FILE = 'Orc/output/snappy_nodes.orc'
ORC_ZLIB_FILE = 'Orc/output/zlib_nodes.orc'
JSON_FILE = 'Orc/output/nodes.json'

# Define data schema
schema = "struct<id:int,longitude:float,latitude:float,username:string>"

nodes = []
tree = ET.parse(open(SOURCE_FILE))
for node in tree.iterfind('node'):
    nodes.append((
        int(node.get('id')),
        float(node.get('lon')),
        float(node.get('lat')),
        node.get('user')
    ))

with open(ORC_FILE, "wb") as data:
    with pyorc.Writer(data, schema, compression=pyorc.CompressionKind.NONE) as writer:
        for node in nodes:
            writer.write(node)

## Looks like SNAPPY and LZO compression aren't supported by ORC yet?
#
# with open(ORC_SNAPPY_FILE, "wb") as data:
#     with pyorc.Writer(data, schema, compression=pyorc.CompressionKind.SNAPPY) as writer:
#         for node in nodes:
#             writer.write(node)
##

with open(ORC_ZLIB_FILE, "wb") as data:
    with pyorc.Writer(data, schema, compression=pyorc.CompressionKind.ZLIB) as writer:
        for node in nodes:
            writer.write(node)

# do the same with JSON format (for comparison)
with open(JSON_FILE, 'w') as json_file:
    json.dump(nodes, json_file)

# Compare the size of the file formats
def print_file_size(file_path):
    file_stats = os.stat(file_path)
    print(f'Size of file {file_path} is {file_stats.st_size}')

print('Comparison of the file size of the different formats and compression algorithms:')
print_file_size(ORC_FILE)
# print_file_size(ORC_SNAPPY_FILE)
print_file_size(ORC_ZLIB_FILE)
print_file_size(JSON_FILE)
