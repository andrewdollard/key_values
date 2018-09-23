import constants
import random
import socket

def make_request(msg, ports):
    response = b''
    port = random.choice(ports)
    count = 0

    while True:
        count += 1
        if count > len(ports):
            break

        try:
            print(f"trying: {port}")
            s = socket.socket(socket.AF_INET)
            s.connect(('localhost', port))
            s.send(msg)
            response = receive(s)
            s.close()
        except:
            port = random.choice(ports)
            continue

        if response[0:1] != constants.FORWARD:
            break
        port = int.from_bytes(response[1:], byteorder='big')
    return response

def receive(socket):
    response = b''
    while True:
        chunk = socket.recv(4096)
        response += chunk
        if len(chunk) < 4096:
            break
    return response
