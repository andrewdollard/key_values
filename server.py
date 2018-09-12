import os
import socket
from net import receive

s = socket.socket(socket.AF_INET)
s.bind(('localhost', 1234))
s.listen(5)


def load_data():
    data = {}
    if not os.path.exists('data_file'):
        return {}

    with open('data_file', 'r+') as data_file:
        for line in data_file.readlines():
            key, value = line.rstrip().split('=')
            data[key] = value
    return data

def store_data(data):
    with open('data_file', 'w+') as data_file:
        for key in data:
            data_file.write('{}={}\n'.format(key, data[key]))


while True:
    (clientsocket, address) = s.accept()
    request = receive(clientsocket)
    print(f"request from {address}: {request}")

    cmd, arg = request.split(' ')

    if cmd == 'get':
        data = load_data()
        if arg in data:
            resp = data[arg] + '\n'
            clientsocket.send(bytes(resp, 'utf-8'))
        else:
            clientsocket.send(bytes('not found\n', 'utf-8'))

    if cmd == 'set':
        data = load_data()
        key, value = arg.split('=')
        data[key] = value
        store_data(data)

