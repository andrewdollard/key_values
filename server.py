import os
import socket
import sys
from io import BytesIO
from net import receive
from serialization import KEY_LENGTH, serialize_record, serialize_catchup, deserialize_records

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

def max_lsn(data):
    return max([data[k]['lsn'] for k in data]) if len(data) > 0 else 0

def update_replica(req):
    replica_socket = socket.socket(socket.AF_INET)
    try:
        replica_socket.connect(('localhost', REPLICA_PORT))
        replica_socket.send(req)
    except:
        print("replica is not available!")
    finally:
        replica_socket.close()


PORT = int(sys.argv[1]) if len(sys.argv) > 1 else 1234
REPLICA_PORT = int(sys.argv[2]) if len(sys.argv) > 2 else None
print(f"port is {PORT}")
print(f"replica port is {REPLICA_PORT}")

DATA_FILE = f"data/{PORT}"
if not os.path.exists('data'):
    os.makedirs('data')

if REPLICA_PORT is not None:
    data = load_data()
    lsn = max_lsn(data)
    try:
        replica_socket = socket.socket(socket.AF_INET)
        replica_socket.connect(('localhost', REPLICA_PORT))
        catchup_request = serialize_catchup(lsn)
        replica_socket.send(catchup_request)
        response = receive(replica_socket)
        data.update(deserialize_records(BytesIO(response)))
        store_data(data)
        print("update from replica complete")
    except Exception as ex:
        print("replica is not available!")
        print(ex)
    finally:
        replica_socket.close()

s = socket.socket(socket.AF_INET)
s.bind(('localhost', PORT))
s.listen(5)

while True:
    (clientsocket, address) = s.accept()
    req = receive(clientsocket)
    print(f"request from {address}: {req}")
    data = load_data()

    if len(req) > KEY_LENGTH:
        key = req[1:KEY_LENGTH + 1]
        if req[0] == 0:
            value = req[KEY_LENGTH + 1:]
            lsn = max_lsn(data)
            new_record = {
                'lsn': lsn + 1,
                'value': value,
            }
            data[key] = new_record
            store_data(data)

            if REPLICA_PORT is not None:
                update_replica(req)

        if req[0] == 1:
            if key in data:
                resp = data[key]['value']
                clientsocket.send(resp)
            else:
                clientsocket.send(bytes('not found\n', 'utf-8'))

    elif req[0] == 2:
        import pdb; pdb.set_trace()
        requested_lsn = int.from_bytes(req[1:], byteorder='big')
        for key in data:
            if data[key]['lsn'] > requested_lsn:
                ba = serialize_record(key, data[key]['value'], data[key]['lsn'])
                clientsocket.send(ba)
        clientsocket.close()

