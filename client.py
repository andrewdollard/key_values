import cmd
import socket

class KVCli(cmd.Cmd):

    def do_set(self, pair):
        s = socket.socket(socket.AF_INET)
        s.connect(('localhost', 1234))
        s.send('set {}\n'.format(pair))
        s.close


    def do_get(self, key):
        s = socket.socket(socket.AF_INET)
        s.connect(('localhost', 1234))
        s.send('get {}\n'.format(key))

        response = ""
        while True:
            chunk = s.recv(4096)
            response += chunk.decode('utf-8')
            if chunk.endswith('\n'):
                break
        s.close
        print(response.rstrip())

interpreter = KVCli()
interpreter.cmdloop()

