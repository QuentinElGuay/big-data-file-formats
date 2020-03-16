from pyorc import Reader
from pyorc.enums import StructRepr

ORC_FILE = 'data/orc/userdata1.orc'

with open(ORC_FILE, 'rb') as orc_file:
    reader = Reader(orc_file)
    
    # Read embedded schema
    print(str(reader.schema))
    
    # Read all the file at once:
    rows = reader.read()
    print(rows)

    # Go back to first line
    reader.seek(0)

    # Read the content of userdata1.orc by batch of 100 records
    # Using this optional parameter for large ORC files is highly recommended!
    rows = reader.read(100)
    while rows:
        print(rows)
        rows = reader.read(100)

    # Read file and return a list of dictionaries
    reader = Reader(orc_file, struct_repr=StructRepr.DICT)
    print(next(reader))

    print(reader.num_of_stripes)
