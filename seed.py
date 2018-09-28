import socket
import random
from serialization import serialize_add_nodes, serialize_set
from net import make_request, simple_send
from time import sleep

KNOWN_PORTS = {1234, 1235}

POSITION_TABLE = {
    0.25: 1234,
    0.50: 1235,
    0.75: 1234,
    1.00: 1235,
}

SEED_DATA = {
    'a': 'b',
    'c': 'd',
    'e': 'e',
    'g': 'h',
    'i': 'j',
    'k': 'l',
    'm': 'n',
    'aa': 'b',
    'ca': 'd',
    'ea': 'e',
    'ga': 'h',
    'ia': 'j',
    'ka': 'l',
    'ma': 'n',
    'ab': 'b',
    'cb': 'd',
    'eb': 'e',
    'gb': 'h',
    'ib': 'j',
    'kb': 'l',
    'mb': 'n',
}

sleep(2)
print("seeding")

for p in KNOWN_PORTS:
    msg = serialize_add_nodes(POSITION_TABLE)
    simple_send(msg, p)

for k in SEED_DATA:
    make_request(serialize_set(k, SEED_DATA[k]), KNOWN_PORTS)

print("seeding complete")
