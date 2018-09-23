import cmd
import constants
import socket
import random
from serialization import serialize_set, serialize_get
from net import receive

KNOWN_PORTS = [1234, 1235, 1236]

def make_request(msg):
    response = b''
    port = random.choice(KNOWN_PORTS)
    count = 0

    while True:
        count += 1
        if count > len(KNOWN_PORTS):
            break

        s = socket.socket(socket.AF_INET)
        s.connect(('localhost', port))
        s.send(msg)
        response = receive(s)
        s.close()

        if response[0:1] != constants.FORWARD:
            break
        port = int.from_bytes(response[1:], byteorder='big')
    return response

class KVCli(cmd.Cmd):

    def do_set(self, pair):
        key, value = pair.split('=')
        payload = serialize_set(key, value)
        response = make_request(payload)
        print(response)

    def do_get(self, key):
        payload = serialize_get(key)
        response = make_request(payload)
        print(response)

        if response[0:1] == constants.GET_OK:
            print(response[1:])
        elif response[0:1] == constants.GET_NOT_FOUND:
            print("value was not found")

interpreter = KVCli()
interpreter.cmdloop()

