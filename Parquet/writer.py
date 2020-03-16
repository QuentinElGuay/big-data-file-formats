import os
from xml.etree import ElementTree as ET

from fastparquet import write
import pandas as pd

SOURCE_FILE = 'data/paris/2.250182,48.818215,2.251182,48.819215.osm'
PARQ_FILE = 'Parquet/output/nodes.parq'
PARQ_SNAPPY_FILE = 'Parquet/output/snappy_nodes.parq'
PARQ_GZIP_FILE = 'Parquet/output/gzip_nodes.parq'
JSON_FILE = 'Parquet/output/nodes.json'

nodes = []
tree = ET.parse(open(SOURCE_FILE))
for node in tree.iterfind('node'):
    nodes.append({
        'id': int(node.get('id')),
        'longitude': float(node.get('lon')),
        'latitude': float(node.get('lat')),
        'username': node.get('user')
    })

df = pd.DataFrame.from_records(nodes)

# Write nodes dictionary in an parquet file
write(PARQ_FILE, df)

# Write nodes dictionary in an avro file and use snappy compression algorithm
write(PARQ_SNAPPY_FILE, df, compression='snappy')

# Write nodes dictionary in an avro file and use GZIP compression algorithm
write(PARQ_GZIP_FILE, df, compression='GZIP')

# do the same with JSON format (for comparison)
df.to_json(JSON_FILE)

# Compare the size of the file formats
def print_file_size(file_path):
    file_stats = os.stat(file_path)
    print(f'Size of file {file_path} is {file_stats.st_size}')

print('Comparison of the file size of the different formats and compression algorithms:')
print_file_size(PARQ_FILE)
print_file_size(PARQ_SNAPPY_FILE)
print_file_size(PARQ_GZIP_FILE)
print_file_size(JSON_FILE)
