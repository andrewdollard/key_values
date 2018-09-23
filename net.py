import constants
import random
import socket

def simple_send(msg, port):
    s = socket.socket(socket.AF_INET)
    s.connect(('localhost', port))
    s.send(msg)
    s.close()

def make_request(msg, original_ports):
    ports = set(original_ports)
    port = ports.pop()
    ports.remove(port)

    dead_ports = set()
    response = b''
    count = 0
    while True:
        count += 1
        if count > len(original_ports):
            break

        try:
            print(f"trying: {port}")
            s = socket.socket(socket.AF_INET)
            s.connect(('localhost', port))
            s.send(msg)
            response = receive(s)
            s.close()
        except:
            dead_ports.add(port)
            port = ports.pop()
            ports.remove(port)
            continue

        if response[0:1] != constants.FORWARD:
            break
        port = int.from_bytes(response[1:], byteorder='big')

    # for dp in dead_ports:
    #     for p in ports:
    #         msg = serialize_remove_node(dp)
    #         simple_send(msg, p)

    return response

def receive(socket):
    response = b''
    while True:
        chunk = socket.recv(4096)
        response += chunk
        if len(chunk) < 4096:
            break
    return response
