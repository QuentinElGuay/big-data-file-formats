import copy
import json
import os
from pprint import pprint
from xml.etree import ElementTree as ET

import fastavro

SOURCE_FILE = 'data/paris/2.250182,48.818215,2.251182,48.819215.osm'
AVRO_FILE = 'Avro/output/nodes.avro'
AVRO_SNAPPY_FILE = 'Avro/output/snappy_nodes.avro'
AVRO_BZIP2_FILE = 'Avro/output/bzip2_nodes.avro'
JSON_FILE = 'Avro/output/nodes.json'

# Define data schema
schema = {
    "type": "record",
    "namespace": "openstreetmap",
    "name": "nodes",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "longitude", "type": "float"},
        {"name": "latitude", "type": "float"},
        {"name": "username", "type": "string"},
    ]
}

nodes = []
tree = ET.parse(open(SOURCE_FILE))
for node in tree.iterfind('node'):
    nodes.append({
        'id': int(node.get('id')),
        'longitude': float(node.get('lon')),
        'latitude': float(node.get('lat')),
        'username': node.get('user')
    })

# Dump nodes dictionary in an avro file
with open(AVRO_FILE, 'wb') as avro_file:
    fastavro.writer(avro_file, schema, nodes)

# Dump nodes dictionary in an avro file and use snappy compression algorithm
with open(AVRO_SNAPPY_FILE, 'wb') as avro_file:
    fastavro.writer(avro_file, schema, nodes, codec='snappy')

# Dump nodes dictionary in an avro file and use Bzip2 compression algorithm
with open(AVRO_BZIP2_FILE, 'wb') as avro_file:
    fastavro.writer(avro_file, schema, nodes, codec='bzip2')

# do the same with JSON format (for comparison)
with open(JSON_FILE, 'w') as json_file:
    json.dump([schema, nodes], json_file)

# Compare the size of the file formats
def print_file_size(file_path):
    file_stats = os.stat(file_path)
    print(f'Size of file {file_path} is {file_stats.st_size}')

print('Comparison of the file size of the different formats and compression algorithms:')
print_file_size(AVRO_FILE)
print_file_size(AVRO_SNAPPY_FILE)
print_file_size(AVRO_BZIP2_FILE)
print_file_size(JSON_FILE)

## Schema evolution
# Update schema to handle "tags" from .osm file
schema2 = copy.deepcopy(schema)
schema2['fields'].append({"name": "tags", "type": {"type": "map", "values": "string"}, "default":[]})

# parse again, adding the tags
tree = ET.parse(open(SOURCE_FILE))
for node in tree.iterfind('node'):
    tags = {}
    for tag in node:
        tags[tag.get('k')] = tag.get('v')

    nodes.append({
        'id': int(node.get('id')),
        'longitude': float(node.get('lon')),
        'latitude': float(node.get('lat')),
        'username': node.get('user'),
        'tags': tags
    })
    
with open('Avro/output/bzip2_nodes_v2.avro', 'wb') as avro_file:
    fastavro.writer(avro_file, schema2, nodes, codec='bzip2')

print_file_size('Avro/output/bzip2_nodes_v2.avro')

# Read Avro file
with open('Avro/output/bzip2_nodes_v2.avro', 'rb') as avro_file:
    reader = fastavro.reader(avro_file, schema2)

    # Set compression codec
    reader.codec='snappy'

    # print file content
    for record in reader:
        pprint(record)