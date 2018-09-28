import constants
import os
import pdb
import random
import socket
import sys
import time
from io import BytesIO
from net import make_request, receive, \
    simple_send, simple_send_and_receive
from persistence import load_data, store_data
from serialization import KEY_LENGTH, KEYSPACE_SIZE,          \
    serialize_catchup, serialize_record, deserialize_records, \
    serialize_add_nodes, deserialize_add_nodes,               \
    serialize_request_partitions, serialize_add_record,       \
    deserialize_remove_node

PORT = int(sys.argv[1])
DATA_FILE = f"data/{PORT}"

try:
    os.makedirs('data')
except:
    pass

known_ports = set([int(arg) for arg in sys.argv[2:]])
print(f"known ports: {known_ports}")

partition_table = {}

def max_lsn(data):
    return max([data[k]['lsn'] for k in data]) if len(data) > 0 else 0

def get_partition(key):
    return int.from_bytes(key, byteorder='big') / KEYSPACE_SIZE

def get_port(partition):
    for node_partition in sorted(partition_table):
        if partition < node_partition:
            node_port = partition_table[node_partition]
            return node_port

def calculate_table_updates():
    node_to_split = random.sample(known_ports, 1)[0]
    result = {}

    base = 0
    for partition in sorted(partition_table):
        if partition_table[partition] == node_to_split:
            new_partition = ((partition - base) / 2) + base
            result[new_partition] = PORT
        base = partition
    return result

def rebalance_data():
    data = load_data(DATA_FILE)
    new_data = {}
    for k in data:
        partition = get_partition(k)
        key_port = get_port(partition)
        if key_port != PORT:
            msg = serialize_add_record(k, data[k]['value'], data[k]['lsn'])
            simple_send(msg, key_port)
        else:
            new_data[k] = data[k]
    store_data(DATA_FILE, new_data)

if known_ports and (PORT not in known_ports):
    print("requesting partition table")
    req = serialize_request_partitions(PORT)
    resp = make_request(req, known_ports)
    partition_table = deserialize_add_nodes(resp)
    print(f"received partition table: {partition_table}")

    known_ports.update([v for v in partition_table.values()])
    table_updates = calculate_table_updates()
    partition_table.update(table_updates)
    print(f"calculated new partition table: {partition_table}")

    for p in known_ports:
        if p == PORT:
            continue
        simple_send(serialize_add_nodes(table_updates), p)

    rebalance_data()

s = socket.socket(socket.AF_INET)
s.bind(('localhost', PORT))
s.listen(5)

while True:
    (clientsocket, address) = s.accept()
    req = receive(clientsocket)
    print(f"request: {req}")
    data = load_data(DATA_FILE)

    if len(req) > KEY_LENGTH:
        key = req[1:KEY_LENGTH + 1]

        key_port = get_port(get_partition(key))

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

        elif req[0:1] == constants.GET:
            if key in data:
                value = data[key]['value']
                clientsocket.send(constants.GET_OK + value)
            else:
                clientsocket.send(constants.GET_NOT_FOUND)

        elif req[0:1] == constants.ADD_RECORD:
            new_records = deserialize_records(BytesIO(req[1:]))
            print(f"adding new records: f{new_records}")
            data.update(new_records)
            store_data(DATA_FILE, data)

    elif req[0:1] == constants.ADD_NODES:
        node_info = deserialize_add_nodes(req)
        partition_table.update(node_info)
        print(f"new partition table: {partition_table}")
        time.sleep(1)
        rebalance_data()

    elif req[0:1] == constants.REQUEST_PARTITIONS:
        print("sending partition table")
        msg = serialize_add_nodes(partition_table)
        clientsocket.send(msg)

    elif req[0:1] == constants.REMOVE_NODE:
        port = deserialize_remove_node(req)
        print(f"removing dead node: {port}")
        new_partition_table = {}
        for partition in partition_table:
            if partition_table[partition] != port:
                new_partition_table[partition] = partition_table[partition]
        partition_table = new_partition_table
        print(f"new partition table: {partition_table}")
        time.sleep(1)
        rebalance_data()

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

