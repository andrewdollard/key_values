import os
import socket
import sys
from io import BytesIO
from net import receive
from serialization import serialize_record, serialize_catchup, deserialize_records

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
            return deserialize_records(data_file)
    return {}

def parse_request(req, clientsocket):
    if len(req) > 20:
        # set
        if req[0] == 0:
            key = req[1:21]
            value = req[21:]
            data = load_data()
            lsn = max([data[k]['lsn'] for k in data]) if len(data) > 0 else 0
            data[key] = {
                'lsn': lsn + 1,
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

        # get
        if req[0] == 1:
            key = req[1:21]
            data = load_data()
            if key in data:
                resp = data[key]['value']
                clientsocket.send(resp)
            else:
                clientsocket.send(bytes('not found\n', 'utf-8'))

    elif req[0] == 2:
        data = load_data()
        requested_lsn = int.from_bytes(req[1:], byteorder='big')
        for key in data:
            if data[key]['lsn'] > requested_lsn:
                ba = serialize_record(key, data[key]['value'], data[key]['lsn'])
                clientsocket.send(ba)
        clientsocket.close()


s = socket.socket(socket.AF_INET)
s.bind(('localhost', PORT))
s.listen(5)

if REPLICA_PORT is not None:
    data = load_data()
    lsn = max([data[k]['lsn'] for k in data]) if len(data) > 0 else 0
    replica_socket = socket.socket(socket.AF_INET)
    replica_socket.connect(('localhost', REPLICA_PORT))
    catchup_request = serialize_catchup(lsn)
    replica_socket.send(catchup_request)
    response = receive(replica_socket)
    data.update(deserialize_records(BytesIO(response)))
    try:
        store_data(data)
    except:
        print("replica is not available!")
    finally:
        print("update from replica complete")
        replica_socket.close()

while True:
    (clientsocket, address) = s.accept()
    req = receive(clientsocket)
    print(f"request from {address}: {req}")
    parse_request(req, clientsocket)

