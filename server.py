import os
import socket
from net import receive

def load_data():
    data = {}

    if not os.path.exists('data_file'):
        return {}

    with open('data_file', 'rb') as data_file:

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

            value_len = int.from_bytes(bytes, byteorder='big')
            data[key] = data_file.read(value_len)

    return data


def store_data(data):
    with open('data_file', 'wb') as data_file:
        for key in data:
            value = data[key]
            data_file.write(key)

            lv = len(value)
            bytes_in_len = (lv.bit_length() + 7) // 8
            lb = lv.to_bytes(bytes_in_len, byteorder='big')

            data_file.write(lb)
            data_file.write(b':')
            data_file.write(value)


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
s.bind(('localhost', 1234))
s.listen(5)

while True:
    (clientsocket, address) = s.accept()
    req = receive(clientsocket)
    (success, command, key, value) = parse_request(req)

    print(f"request from {address}: {success} {command} {key} {value}")

    if command == 'get':
        data = load_data()
        if key in data:
            resp = data[key]
            clientsocket.send(resp)
        else:
            clientsocket.send(bytes('not found\n', 'utf-8'))

    elif command == 'set':
        data = load_data()
        data[key] = value
        store_data(data)

