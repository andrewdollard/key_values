import cmd

class KVCli(cmd.Cmd):

    data = {}

    def do_set(self, pair):
        key, value = pair.split('=')
        self.data[key] = value

    def do_get(self, key):
        if key in self.data:
            print(self.data[key])
        else:
            print('not found')

interpreter = KVCli()
interpreter.cmdloop()

