import cmd
import socket
from net import receive

class KVCli(cmd.Cmd):

    def do_set(self, pair):
        s = socket.socket(socket.AF_INET)
        s.connect(('localhost', 1234))
        req = 'set {}\n'.format(pair)
        s.send(bytes(req, 'utf-8'))
        s.close


    def do_get(self, key):
        s = socket.socket(socket.AF_INET)
        s.connect(('localhost', 1234))
        req = 'get {}\n'.format(key)
        s.send(bytes(req, 'utf-8'))

        response = receive(s)
        print(response)

interpreter = KVCli()
interpreter.cmdloop()

