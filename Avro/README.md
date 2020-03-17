# Avro file format
Experimentations with Avro.

### What is Avro?
Apache Avro is a row-based data format released by Hadoop working group in 2009. 
Row-based formats are optimized for fast writing therefore they are recommended for storing raw streaming data or as an intermediary structured format for batch processing. 

Avro stores the data in a binary format and embeds the data schema as JSON (which means human-readable) in the file header. By doing so, Avro keeps an on-write data structure. It also allows [schema evolution](https://avro.apache.org/docs/current/spec.html#Schema+Resolution) in an easy way with retrocompatibility when required.

Avro is the less optimized format for compression but it allows using the `snappy`, `bzip2` and `deflate` algorithms ((documentation](https://avro.apache.org/docs/1.8.1/api/java/org/apache/avro/file/CodecFactory.html)). 

#### Kafka
Avro is the [recommended](https://www.confluent.io/blog/avro-kafka-data/) file format for Kafka as the main goal of Kafka is collecting and writing data.

### Avro and Python
I used the fastavro library which is very simple to use. To use snappy, you will need to install the `libsnappy-dev` package and the `python-snappy` library.

#### Prerequisites
+ libsnappy-dev
+ [fastavro](https://pypi.org/project/fastavro/)
+ [python-snappy](https://pypi.org/project/python-snappy/)

Python libs:
``` bash
sudo apt-get install libsnappy-dev
pip install fastavro
pip install python-snappy
```

