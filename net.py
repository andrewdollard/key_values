def receive(socket):
    response = ""
    while True:
        chunk = socket.recv(4096)
        response += chunk.decode('utf-8')
        if response.endswith('\n'):
            break
    return response.rstrip()
