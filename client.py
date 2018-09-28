import cmd
import constants
from serialization import serialize_set, serialize_get
from net import make_request

KNOWN_PORTS = {1234, 1235, 1236}

class KVCli(cmd.Cmd):

    def do_set(self, pair):
        key, value = pair.split('=')
        payload = serialize_set(key, value)
        response = make_request(payload, KNOWN_PORTS)
        print(response)

    def do_get(self, key):
        payload = serialize_get(key)
        response = make_request(payload, KNOWN_PORTS)

        if response[0:1] == constants.GET_OK:
            print(response[1:])
        elif response[0:1] == constants.GET_NOT_FOUND:
            print("value was not found")

interpreter = KVCli()
interpreter.cmdloop()

