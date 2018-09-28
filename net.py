import constants
import pdb
import random
import socket
from collections import deque

from serialization import serialize_remove_node, deserialize_forward

def simple_send(msg, port):
    s = socket.socket(socket.AF_INET)
    try:
        s.connect(('localhost', port))
        s.send(msg)
    except:
        print(f"port {port} is not responding!")
    finally:
        s.close()

def simple_send_and_receive(req, port):
    s = socket.socket(socket.AF_INET)
    response = None
    try:
        s.connect(('localhost', port))
        s.send(req)
        response = receive(s)
    except Exception as e:
        print(f"port {port} is not responding!")
        print(e)
    finally:
        s.close()
    return response

def make_request(msg, original_ports):
    random.shuffle(original_ports)
    working_ports = deque(original_ports)
    dead_ports = []
    response = b''

    while True:
        if len(working_ports) == 0:
            break

        port = working_ports.pop()
        print(f"trying: {port}")
        response = simple_send_and_receive(msg, port)
        if response is None:
            dead_ports.append(port)
            continue

        if response[0:1] != constants.FORWARD:
            break
        ports = deserialize_forward(response)
        working_ports.extend(reversed(ports))

    for dp in dead_ports:
        for p in original_ports:
            if p == dp:
                continue
            print(f"asking {p} to remove dead node: {dp}")
            msg = serialize_remove_node(dp)
            simple_send(msg, p)

    return response

def receive(socket):
    response = b''
    while True:
        chunk = socket.recv(4096)
        response += chunk
        if len(chunk) < 4096:
            break
    return response
