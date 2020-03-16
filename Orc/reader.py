import pyorc

ORC_FILE = 'data/orc/userdata1.orc'

with open(ORC_FILE, 'rb') as orc_file:
    reader = pyorc.Reader(orc_file)

    print(str(reader.schema))
    #print(reader.read())