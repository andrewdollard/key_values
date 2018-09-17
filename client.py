import cmd
import socket
from net import receive
from hashlib import blake2b as blake

def serialize_set(key, value):
    hasher = blake(digest_size=20)
    hasher.update(bytes(key, 'utf-8'))
    key_bytes = hasher.digest()
    value_bytes = bytes(value, 'utf-8')

    return b'\x00' + key_bytes + value_bytes

def serialize_get(key):
    hasher = blake(digest_size=20)
    hasher.update(bytes(key, 'utf-8'))
    key_bytes = hasher.digest()

    return b'\x01' + key_bytes

def open_socket():
    s = socket.socket(socket.AF_INET)
    try:
        s.connect(('localhost', 1234))
    except:
        s = socket.socket(socket.AF_INET)
        s.connect(('localhost', 1235))

    return s

class KVCli(cmd.Cmd):

    def do_set(self, pair):
        key, value = pair.split('=')
        payload = serialize_set(key, value)

        s = open_socket()
        s.send(payload)
        s.close()


    def do_get(self, key):
        payload = serialize_get(key)

        s = open_socket()
        s.send(payload)
        response = receive(s)
        s.close()

        print(response)

interpreter = KVCli()
interpreter.cmdloop()

