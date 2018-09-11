import os
import socket

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
    print(f"request from {address}")

    response = ""
    while True:
        chunk = clientsocket.recv(4096)
        response += chunk.decode('utf-8')
        if response.endswith('\n'):
            break
    print(response)
    cmd, arg = response.rstrip().split(' ')

    if cmd == 'get':
        data = load_data()
        if arg in data:
            resp = data[arg] + '\n'
            clientsocket.send(bytes(resp, 'utf-8'))
        else:
            clientsocket.send('not found\n')

    if cmd == 'set':
        data = load_data()
        key, value = arg.split('=')
        data[key] = value
        store_data(data)

