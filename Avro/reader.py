from pprint import pprint
import sys

import fastavro

avro_file = sys.argv[1]
schema = sys.argv[2]
compression_codec = 'null'
if len(sys.argv) > 3:
    compression_codec = sys.argv[3]

# Read Avro schema
schema = fastavro.schema.load_schema(schema)

# Read Avro file
with open(avro_file, 'rb') as avro_file:
    reader = fastavro.reader(avro_file, schema)
    
    # Set compression codec
    reader.codec=compression_codec

    # print file content
    for record in reader:
        pprint(record)

