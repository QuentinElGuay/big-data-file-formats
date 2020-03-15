import fastavro
from pprint import pprint

AVRO_FILE = 'data/avro/userdata1.avro'
AVRO_SCHEMA = 'data/avro/user.avsc'

# Read Avro schema
schema = fastavro.schema.load_schema(AVRO_SCHEMA)

# Read Avro file
with open(AVRO_FILE, 'rb') as avro_file:
    reader = fastavro.reader(avro_file, schema)
    
    # Set compression codec
    reader.codec='snappy'

    # print file content
    for record in reader:
        pprint(record)

