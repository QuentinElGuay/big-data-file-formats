from fastparquet import ParquetFile

PARQ_FILE = 'data/parquet/userdata1.parquet'

pf = ParquetFile(PARQ_FILE)
df = pf.to_pandas()

# print file content
print(df.head)
