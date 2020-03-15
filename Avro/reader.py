import fastavro
from pprint import pprint

AVRO_FILE = 'data/avro/userdata1.avro'

with open(AVRO_FILE, 'rb') as avro_file:
    reader = fastavro.reader(avro_file)
    
    # Discover schema
    pprint(reader.schema)

    # print file
    for kylosample in reader:
        print(kylosample)

