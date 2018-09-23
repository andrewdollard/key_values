import constants
from enum import Enum
from hashlib import blake2b as blake

KEY_LENGTH = 20
KEYSPACE_SIZE = 2**(KEY_LENGTH * 8)

def serialize_set(key, value):
    hasher = blake(digest_size=KEY_LENGTH)
    hasher.update(bytes(key, 'utf-8'))
    key_bytes = hasher.digest()
    value_bytes = bytes(value, 'utf-8')

    return constants.SET + key_bytes + value_bytes

def serialize_get(key):
    hasher = blake(digest_size=KEY_LENGTH)
    hasher.update(bytes(key, 'utf-8'))
    key_bytes = hasher.digest()

    return constants.GET + key_bytes

def deserialize_get_response(resp):
    return resp[1:]

def serialize_catchup(lsn):
    bytes_in_lsn = (lsn.bit_length() + 7) // 8
    lsnb = lsn.to_bytes(bytes_in_lsn, byteorder='big')
    return constants.CATCHUP + lsnb

def serialize_record(key, value, lsn):
    ba = bytearray()

    bytes_in_lsn = (lsn.bit_length() + 7) // 8
    lsnb = lsn.to_bytes(bytes_in_lsn, byteorder='big')

    lv = len(value)
    bytes_in_len = (lv.bit_length() + 7) // 8
    lb = lv.to_bytes(bytes_in_len, byteorder='big')

    ba.extend(key)
    ba.extend(lsnb)
    ba.extend(b':')
    ba.extend(lb)
    ba.extend(b':')
    ba.extend(value)
    return ba

def deserialize_records(stream):
    data={}
    while True:
        key = stream.read(KEY_LENGTH)
        if len(key) < KEY_LENGTH:
            break

        lsn = read_int(stream)
        value_len = read_int(stream)

        data[key] = {
            'lsn': lsn,
            'value': stream.read(value_len),
        }

    return data

def read_int(stream):
    ary = []
    while True:
        b = stream.read(1)
        if b == b':':
            break
        else:
            ary += b
    return int.from_bytes(ary, byteorder='big')

