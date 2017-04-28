# for consensus, election, and send load

import socket
import thread


def send_message(ip, port, message):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((ip, port))
    s.send(message)
    s.close()


def handle_message(ip, port, nConn):
    end = False
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((ip, port))
    s.listen(nConn)
    while 1:
        conn, addr = s.accept()
        while 1:
            data = conn.recv(BUFFER_SIZE)
            if not data: break

            # handle the message here
            if 1:
                print("received data:", data)

    conn.close()


# how to use
TCP_IP = '127.0.0.1'
BUFFER_SIZE = 1024

LISTEN_PORT = int(raw_input())
SEND_PORT = int(raw_input())

try:
    thread.start_new_thread(handle_message, (TCP_IP, LISTEN_PORT, 1))
except:
    print("Error: unable to start thread"
          )

while 1:
    message = raw_input()
    if message == "close": break
    send_message(TCP_IP, SEND_PORT, message)
