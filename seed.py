import socket
from serialization import serialize_add_node
from time import sleep

KNOWN_PORTS = [1234, 1235, 1236]

POSITION_TABLE = {
    0.11: 1234,
    0.22: 1235,
    0.33: 1236,
    0.44: 1234,
    0.55: 1235,
    0.66: 1236,
    0.77: 1234,
    0.88: 1235,
    1.00: 1236,
}

sleep(2)
print("seeding")

for p in KNOWN_PORTS:
    for np in POSITION_TABLE:
        s = socket.socket(socket.AF_INET)
        s.connect(('localhost', p))
        msg = serialize_add_node(np, POSITION_TABLE[np])
        s.send(msg)
        s.close()

print("seeding complete")
