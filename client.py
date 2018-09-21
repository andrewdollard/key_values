import cmd
import constants
import socket
from serialization import serialize_set, serialize_get
from net import receive

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
        response = receive(s)
        s.close()


    def do_get(self, key):
        payload = serialize_get(key)

        s = open_socket()
        s.send(payload)
        response = receive(s)
        s.close()

        if response[0:1] == constants.GET_OK:
            print(response[1:])
        elif response[0:1] == constants.GET_NOT_FOUND:
            print("value was not found")

interpreter = KVCli()
interpreter.cmdloop()

