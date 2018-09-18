import os
import socket
import sys
from net import receive
from serialization import serialize_record, serialize_catchup

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 1234
REPLICA_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else None

print(f"port is {PORT}")
print(f"replica port is {REPLICA_PORT}")

DATA_FILE = f"data/{PORT}"
if not os.path.exists('data'):
    os.makedirs('data')

def store_data(data):
    with open(DATA_FILE, 'wb') as data_file:
        for key in data:
            b = serialize_record(key, data[key]['value'], data[key]['lsn'])
            data_file.write(b)

def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, 'rb') as data_file:
            return read_data(data_file)
    return {}

def read_data(data_file):
    data={}
    while True:
        key = data_file.read(20)
        if len(key) < 20:
            break

        bytes = []
        while True:
            b = data_file.read(1)
            if b == b':':
                break
            else:
                bytes += b
        lsn = int.from_bytes(bytes, byteorder='big')

        bytes = []
        while True:
            b = data_file.read(1)
            if b == b':':
                break
            else:
                bytes += b
        value_len = int.from_bytes(bytes, byteorder='big')

        data[key] = {
            'lsn': lsn,
            'value': data_file.read(value_len),
        }

    return data

def parse_request(req):
    success = False
    command = None
    key = None
    value = None

    print(req)

    if len(req) > 20:
        if req[0] == 0:
            success = True
            command = 'set'
            key = req[1:21]
            value = req[21:]

        if req[0] == 1:
            success = True
            command = 'get'
            key = req[1:21]

    return (success, command, key, value)


s = socket.socket(socket.AF_INET)
s.bind(('localhost', PORT))
s.listen(5)

while True:
    (clientsocket, address) = s.accept()
    req = receive(clientsocket)
    (success, command, key, value) = parse_request(req)

    print(f"request from {address}: {success} {command} {key} {value}")

    if command == 'get':
        data = load_data()
        if key in data:
            resp = data[key]['value']
            clientsocket.send(resp)
        else:
            clientsocket.send(bytes('not found\n', 'utf-8'))

    elif command == 'set':
        data = load_data()

        lsn = max([data[k]['lsn'] for k in data]) + 1 if len(data) > 0 else 1
        data[key] = {
            'lsn': lsn,
            'value': value,
        }

        store_data(data)

        if REPLICA_PORT is not None:
            replica_socket = socket.socket(socket.AF_INET)
            try:
                replica_socket.connect(('localhost', REPLICA_PORT))
                replica_socket.send(req)
            except:
                print("replica is not available!")
            finally:
                replica_socket.close()

