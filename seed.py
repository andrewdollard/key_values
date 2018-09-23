import socket
from serialization import serialize_add_node
from time import sleep

KNOWN_PORTS = [1234, 1235]

POSITION_TABLE = {
    0.50: 1234,
    1.00: 1235,
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
