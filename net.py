def receive(socket):
    response = b''
    while True:
        chunk = socket.recv(4096)
        response += chunk
        if len(chunk) < 4096:
            break
    return response
