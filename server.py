import logging
import sys
logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format='%(message)s')
logging.getLogger("requests").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

import constants
import os
import pdb
import pprint
import random
import requests
import socket
import subprocess
import time
import threading
from io import BytesIO
from net import make_request, receive, \
    simple_send, simple_send_and_receive
from persistence import load_data, store_data
from serialization import KEY_LENGTH, KEYSPACE_SIZE,          \
    serialize_catchup, serialize_record, deserialize_records, \
    serialize_add_nodes, deserialize_add_nodes,               \
    serialize_request_partitions, serialize_add_record,       \
    serialize_forward, deserialize_remove_node,               \
    deserialize_catchup

PORT = int(sys.argv[1])
DATA_FILE = f"data/{PORT}"
ETCD_HOST = subprocess.getoutput("cd docker-compose-etcd && bin/service_address.sh etcd0 2379")
LSN_URL = f"http://{ETCD_HOST}/v2/keys/lsn"

try:
    os.makedirs('data')
except:
    pass

REPLICATION_FACTOR = 2

known_ports = [int(arg) for arg in sys.argv[2:]]
logging.info(f"known ports: {known_ports}")

partition_table = {}

def max_lsn(data):
    return max([data[k]['lsn'] for k in data]) if len(data) > 0 else 0

def get_location(key):
    return int.from_bytes(key, byteorder='big') / KEYSPACE_SIZE

def get_replica_node_ports(primary_port):
    known_ports = [v for v in partition_table.values()]
    ports = []
    for i in range(0, len(known_ports)):
        if known_ports[i] == primary_port:
            j = i
            while True:
                if (j - i) == REPLICATION_FACTOR:
                    break
                j += 1
                ports.append(known_ports[j % len(known_ports)])
    return ports

def get_ports(location):
    partition_bounds = sorted(partition_table)
    ports = []
    for i in range(0, len(partition_bounds)):
        if location < partition_bounds[i]:
            ports.append(partition_table[partition_bounds[i]])
            break

    while True:
        if len(ports) == REPLICATION_FACTOR:
            break
        i = (i + 1) % len(partition_bounds)
        bound = partition_bounds[i]
        ports.append(partition_table[bound])

    return ports


def calculate_table_updates():
    node_to_split = random.choice(known_ports)
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
    send_count = 0
    for k in data:
        location = get_location(k)
        # this needs to only send data to nodes that need it??
        for p in get_ports(location):
            if p != PORT:
                print(f"sending record at location {location} to port {p}")
                msg = serialize_add_record(k, data[k]['value'], data[k]['lsn'])
                simple_send(msg, p)
                send_count += 1
            else:
                new_data[k] = data[k]
    store_data(DATA_FILE, new_data)
    logging.info(f"sent {send_count} records")

if known_ports:
    logging.info("requesting partition table")
    req = serialize_request_partitions(PORT)
    resp = make_request(req, known_ports)
    partition_table = deserialize_add_nodes(resp)
    logging.info(f"received partition table: {sorted(partition_table.items())}")
    known_ports = [v for v in partition_table.values()]
    if PORT not in known_ports:
        table_updates = calculate_table_updates()
        partition_table.update(table_updates)
        logging.info(f"calculated new partition table: {sorted(partition_table.items())}")
        threading.Thread(target=rebalance_data).start()
        for p in known_ports:
            if p == PORT:
                continue
            msg = serialize_add_nodes(table_updates)
            threading.Thread(target=simple_send, args=(msg, p)).start()
    else:
        data = load_data(DATA_FILE)
        for p in known_ports:
            if p == PORT:
                continue
            lsn = max_lsn(data)
            msg = serialize_catchup(lsn, PORT)
            logging.info(f"requesting lsn {lsn} from port {p}")
            response = simple_send_and_receive(msg, p)
            data.update(deserialize_records(BytesIO(response)))
        store_data(DATA_FILE, data)
        logging.info("update from replicas complete")

s = socket.socket(socket.AF_INET)
try:
    s.bind(('localhost', PORT))
except:
    logging.info(f"could not listen on port {PORT}")
    exit

def get_next_lsn():
    while True:
        resp = requests.get(LSN_URL)
        current_lsn = int(resp.json()['node']['value'])
        resp = requests.put(f"{LSN_URL}?prevValue={current_lsn}",
                data={ 'value': current_lsn + 1 })
        if resp.status_code == 200:
            return current_lsn + 1


s.listen(5)

while True:
    (clientsocket, address) = s.accept()
    req = receive(clientsocket)
    # logging.info(f"request from {address}: {req}")
    data = load_data(DATA_FILE)

    if len(req) > KEY_LENGTH:
        key = req[1:KEY_LENGTH + 1]
        key_ports = get_ports(get_location(key))

        if PORT not in key_ports:
            msg = serialize_forward(key_ports)
            clientsocket.send(msg)
            logging.info(f"forwarding to {key_ports}")

        elif req[0:1] == constants.SET:
            value = req[KEY_LENGTH + 1:]
            lsn = get_next_lsn()
            new_record = {
                'lsn': lsn,
                'value': value,
            }

            data[key] = new_record
            location = get_location(key)
            logging.info(f"setting {key}={value} at lsn {lsn}, location {location}")
            ports = get_ports(location)
            logging.info(f"replicating to ports {ports}")
            for p in ports:
                if p != PORT:
                    msg = serialize_add_record(key, new_record['value'], new_record['lsn'])
                    threading.Thread(target=simple_send, args=(msg, p)).start()

            store_data(DATA_FILE, data)

        elif req[0:1] == constants.GET:
            if key in data:
                value = data[key]['value']
                clientsocket.send(constants.GET_OK + value)
            else:
                clientsocket.send(constants.GET_NOT_FOUND)

        elif req[0:1] == constants.ADD_RECORD:
            new_records = deserialize_records(BytesIO(req[1:]))
            logging.info(f"adding new records: f{new_records}")
            data.update(new_records)
            store_data(DATA_FILE, data)

    elif req[0:1] == constants.REQUEST_PARTITIONS:
        logging.info("sending partition table")
        msg = serialize_add_nodes(partition_table)
        clientsocket.send(msg)

    elif req[0:1] == constants.ADD_NODES:
        node_info = deserialize_add_nodes(req)
        partition_table.update(node_info)
        logging.info(f"new partition table: {sorted(partition_table.items())}")
        threading.Thread(target=rebalance_data).start()

    elif req[0:1] == constants.REMOVE_NODE:
        port = deserialize_remove_node(req)
        logging.info(f"removing dead node: {port}")
        new_partition_table = {}
        for partition in partition_table:
            if partition_table[partition] != port:
                new_partition_table[partition] = partition_table[partition]
        partition_table = new_partition_table
        logging.info(f"new partition table: {sorted(partition_table.items())}")
        time.sleep(2)
        threading.Thread(target=rebalance_data).start()

    elif req[0:1] == constants.CATCHUP:
        requested_port, requested_lsn = deserialize_catchup(req)
        logging.info(f"returning lsn {requested_lsn}")
        for key in data:
            belongs_to_node = requested_port in get_ports(get_location(key))
            not_seen = data[key]['lsn'] > requested_lsn
            if belongs_to_node and not_seen:
                ba = serialize_record(key, data[key]['value'], data[key]['lsn'])
                clientsocket.send(ba)

    elif req[0:1] == constants.REPORT:
        logging.info("** data **")
        logging.info(pprint.pformat(data))

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
#         logging.info("update from replica complete")
#     except Exception as ex:
#         logging.info("replica is not available!")
#         logging.info(ex)
#     finally:
#         replica_socket.close()

