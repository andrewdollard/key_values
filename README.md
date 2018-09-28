## To run

`./cluster.sh -d` : deletes data file, kills any existing TCP connections, starts 2 nodes (on ports 1234 & 1235) and runs a seed script

`python server.py 1236 1234 1235`: starts another node on port 1236, and tells it about presence of nodes on 1234 and 1235

`python client.py`: starts a client with knowledge of all three existing nodes.


## Commands

`set foo=bar`

`get foo`

