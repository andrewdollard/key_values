import os
from serialization import serialize_record, deserialize_records

def store_data(f, data):
    with open(f, 'wb') as data_file:
        for key in data:
            b = serialize_record(key, data[key]['value'], data[key]['lsn'])
            data_file.write(b)

def load_data(f):
    if os.path.exists(f):
        with open(f, 'rb') as data_file:
            return deserialize_records(data_file)
    return {}

