from hashlib import blake2b as blake

def serialize_set(key, value):
    hasher = blake(digest_size=20)
    hasher.update(bytes(key, 'utf-8'))
    key_bytes = hasher.digest()
    value_bytes = bytes(value, 'utf-8')

    return b'\x00' + key_bytes + value_bytes

def serialize_get(key):
    hasher = blake(digest_size=20)
    hasher.update(bytes(key, 'utf-8'))
    key_bytes = hasher.digest()

    return b'\x01' + key_bytes

def serialize_catchup(lsn):
    bytes_in_lsn = (lsn.bit_length() + 7) // 8
    lsnb = lsn.to_bytes(bytes_in_lsn, byteorder='big')
    return b'\x02' + lsnb

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

def deserialize_records(records):
    data={}
    while True:
        key = records.read(20)
        if len(key) < 20:
            break

        bytes = []
        while True:
            b = records.read(1)
            if b == b':':
                break
            else:
                bytes += b
        lsn = int.from_bytes(bytes, byteorder='big')

        bytes = []
        while True:
            b = records.read(1)
            if b == b':':
                break
            else:
                bytes += b
        value_len = int.from_bytes(bytes, byteorder='big')

        data[key] = {
            'lsn': lsn,
            'value': records.read(value_len),
        }

    return data
