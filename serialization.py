import constants
import math
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

def serialize_add_record(key, value, lsn):
	return constants.ADD_RECORD + serialize_record(key, value, lsn)

def serialize_get(key):
    hasher = blake(digest_size=KEY_LENGTH)
    hasher.update(bytes(key, 'utf-8'))
    key_bytes = hasher.digest()

    return constants.GET + key_bytes

def serialize_remove_node(port):
	port_bytes = port.to_bytes(2, byteorder='big')
	return constants.REMOVE_NODE + port_bytes

def deserialize_remove_node(req):
	return int.from_bytes(req[1:3], byteorder='big')

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

def serialize_add_nodes(table):
	resp = bytearray(constants.ADD_NODES)
	for k in table:
		node_point_bytes = math.floor(k * 2**16 - 1).to_bytes(2, byteorder='big')
		port_bytes = table[k].to_bytes(2, byteorder='big')
		resp.extend(node_point_bytes)
		resp.extend(port_bytes)
	return resp

def deserialize_add_nodes(req):
	table = {}
	i = 1
	while i < len(req):
		node_point = int.from_bytes(req[i:i+2], byteorder='big')
		port = int.from_bytes(req[i+2:i+4], byteorder='big')
		table.update({ ((node_point + 1) / 2**16): port })
		i += 4
	return table

def serialize_forward(ports):
    resp = bytearray(constants.FORWARD)
    for p in ports:
        resp.extend(p.to_bytes(2, byteorder='big'))
    return resp

def deserialize_forward(req):
    resp = []
    for i in range(1, len(req) - 1, 2):
        resp.append(int.from_bytes(req[i:i+2], byteorder='big'))
    return resp


def serialize_request_partitions(reply_port):
	port_bytes = reply_port.to_bytes(2, byteorder='big')
	return constants.REQUEST_PARTITIONS + port_bytes

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

