import json
import os
from xml.etree import ElementTree as ET

import fastavro

SOURCE_FILE = 'data/paris/2.250182,48.818215,2.251182,48.819215.osm'
AVRO_FILE = 'output/nodes.avro'
JSON_FILE = 'output/nodes.json'

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
    # Ecriture des données
    fastavro.writer(avro_file, schema, nodes)

# do the same with JSON format (for comparison)
with open(JSON_FILE, 'w') as json_file:
    json.dump([schema, nodes], json_file)

# Compare the size of the file formats
def print_file_size(file_path):
    file_stats = os.stat(file_path)
    print(f'Size of file {file_path} is {file_stats.st_size}')

print_file_size(AVRO_FILE)
print_file_size(JSON_FILE)
