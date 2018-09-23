import os
import socket
import sys
import constants
from io import BytesIO
from net import receive
from persistence import load_data, store_data
from serialization import KEY_LENGTH, KEYSPACE_SIZE, \
    serialize_record, serialize_catchup, deserialize_records, deserialize_add_node

PORT = int(sys.argv[1])
DATA_FILE = f"data/{PORT}"

try:
    os.makedirs('data')
except:
    pass

position_table = {}

def update_replica(req):
    replica_socket = socket.socket(socket.AF_INET)
    try:
        replica_socket.connect(('localhost', REPLICA_PORT))
        replica_socket.send(req)
    except:
        print("replica is not available!")
    finally:
        replica_socket.close()

def max_lsn(data):
    return max([data[k]['lsn'] for k in data]) if len(data) > 0 else 0

def find_port(position):
    for node_position in position_table:
        if position < node_position:
            node_port = position_table[node_position]
            print(f"position {position} is at port {node_port}")
            return node_port

s = socket.socket(socket.AF_INET)
s.bind(('localhost', PORT))
s.listen(5)

while True:
    (clientsocket, address) = s.accept()
    req = receive(clientsocket)
    print(f"request from {address}:")
    print(req)
    data = load_data(DATA_FILE)

    if len(req) > KEY_LENGTH:
        key = req[1:KEY_LENGTH + 1]

        position = int.from_bytes(key, byteorder='big') / KEYSPACE_SIZE
        key_port = find_port(position)

        if key_port != PORT:
            resp = key_port.to_bytes(2, byteorder='big')
            clientsocket.send(constants.FORWARD + resp)
            print(f"forwarding to {resp}")

        elif req[0:1] == constants.SET:
            value = req[KEY_LENGTH + 1:]
            lsn = max_lsn(data)
            new_record = {
                'lsn': lsn + 1,
                'value': value,
            }
            data[key] = new_record
            store_data(DATA_FILE, data)
            # update_replica(req)

        elif req[0:1] == constants.GET:
            if key in data:
                value = data[key]['value']
                clientsocket.send(constants.GET_OK + value)
            else:
                clientsocket.send(constants.GET_NOT_FOUND)

    elif req[0:1] == constants.ADD_NODE:
        node_info = deserialize_add_node(req)
        position_table.update(node_info)

    elif req[0:1] == constants.CATCHUP:
        requested_lsn = int.from_bytes(req[1:], byteorder='big')
        for key in data:
            if data[key]['lsn'] > requested_lsn:
                ba = serialize_record(key, data[key]['value'], data[key]['lsn'])
                clientsocket.send(ba)

    clientsocket.close()

# if REPLICA_PORT is not None:
#     data = load_data(DATA_FILE)
#     lsn = max_lsn(data)
#     try:
#         replica_socket = socket.socket(socket.AF_INET)
#         replica_socket.connect(('localhost', REPLICA_PORT))
#         catchup_request = serialize_catchup(lsn)
#         replica_socket.send(catchup_request)
#         response = receive(replica_socket)
#         data.update(deserialize_records(BytesIO(response)))
#         store_data(DATA_FILE, data)
#         print("update from replica complete")
#     except Exception as ex:
#         print("replica is not available!")
#         print(ex)
#     finally:
#         replica_socket.close()

