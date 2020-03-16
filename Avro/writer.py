import sys
from xml.etree import ElementTree as ET

import fastavro

osm_file = sys.argv[1]
schema = schema = fastavro.schema.load_schema(sys.argv[2])
output_folder = sys.argv[3]
compression_codec = 'null'
if len(sys.argv) > 4:
    compression_codec = sys.argv[4]

nodes = []

tree = ET.parse(open(osm_file))

for node in tree.iterfind('node'):
    nodes.append({
        'id': int(node.get('id')),
        'longitude': float(node.get('lon')),
        'latitude': float(node.get('lat')),
        'username': node.get('user')
    })

# Dump nodes dictionary in an avro file
osm_file_name = osm_file.split('/')[-1]
avro_file = output_folder+osm_file_name[:-3]+'avro'
with open(avro_file, 'wb') as af:
    fastavro.writer(af, schema, nodes, codec=compression_codec)
