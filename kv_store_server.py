import cmd
import os

class KVCli(cmd.Cmd):

    def load_data(self):
        data = {}
        if not os.path.exists('data_file'):
            return {}

        with open('data_file', 'r+') as data_file:
            for line in data_file.readlines():
                key, value = line.rstrip().split('=')
                data[key] = value
        return data

    def store_data(self, data):
        with open('data_file', 'w+') as data_file:
            for key in data:
                data_file.write('{}={}\n'.format(key, data[key]))

    def do_set(self, pair):
        data = self.load_data()
        key, value = pair.split('=')
        data[key] = value
        self.store_data(data)

    def do_get(self, key):
        data = self.load_data()
        if key in data:
            print(data[key])
        else:
            print('not found')

interpreter = KVCli()
interpreter.cmdloop()

