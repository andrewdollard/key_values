import cmd

class KVCli(cmd.Cmd):
    def preloop(self):
        print("hello")

    def do_set(self, pair):
        print("setting")

    def do_get(self, pair):
        print("getting")

interpreter = KVCli()
interpreter.cmdloop()

