import socket
import threading

db = {}
host = 'localhost'
port = 6379

def set_value():
    db[key] = set_value
    return 'OK'

def get_value(key):
    return db.get(key, "(nil)")

def delete_key(key):
    if key in db:   
        del db[key]
        return 'OK'
    else:
        return "(nil)"

def start_server():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind((host, port))
    server.listen(5)
    print(f"Server listening on {host}:{port}")
    while True: # Listen for requests
        client_sock, client_address = server.accept()
        print(f"Accepted connection from {client_address}")
        # Create thread to handle request
        client_handler = threading.Thread(
            target = handle_client_connection,
            args = client_sock,
        )
        client_handler.start()

def handle_command(command):
    command_parts = command.split() # Split by spaces
    if len(command_parts) == 0:
        return ""
    command_type = command_parts[0].upper() # Get command type
    if command_type == 'SET':
        if len(command_parts) != 3: # Check if set format is correct
            return "ERROR" 
        return set_value(command_parts[1], command_parts[2])
    elif command_type == 'GET':
        if len(command_parts) != 3: # Check if get format is correct
            return "ERROR" 
        return get_value(command_parts[1])
    elif command_type == 'DEL':
        if len(command_parts) != 3: # Check if delete format is correct
            return "ERROR" 
        return delete_key(command_parts[1])
    else:
        return "ERROR"

def handle_client_connection(client_socket):
    while True:
        command = client_socket.recv(1024).decode() # Read data from client's request
        if not command:
            break 
        result = handle_command(command)
        client_socket.send(result.encode()) # Send back response
    client_socket.close() # Close connection if client disconnects

if __name__ == '__main__':
   start_server()