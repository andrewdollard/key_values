import constants
import pdb
import random
import socket

from serialization import serialize_remove_node

def simple_send(msg, port):
    s = socket.socket(socket.AF_INET)
    s.connect(('localhost', port))
    s.send(msg)
    s.close()

def make_request(msg, original_ports):
    working_ports = set(original_ports)
    port = random.sample(working_ports, 1)[0]

    dead_ports = set()
    response = b''
    while True:
        if len(working_ports) == 0:
            break

        working_ports.remove(port)
        print(f"trying: {port}")
        s = socket.socket(socket.AF_INET)
        try:
            s.connect(('localhost', port))
            s.send(msg)
            response = receive(s)
        except Exception as e:
            print(f"failed to connect: {e}")
            dead_ports.add(port)
            port = random.sample(working_ports, 1)[0]
            continue
        finally:
            s.close()

        if response[0:1] != constants.FORWARD:
            break
        port = int.from_bytes(response[1:], byteorder='big')

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
